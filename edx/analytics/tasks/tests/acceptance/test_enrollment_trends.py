"""
End to end test of enrollment trends.
"""

import datetime
import logging

from luigi.s3 import S3Target
from luigi.date_interval import Custom

from edx.analytics.tasks.tests.acceptance import AcceptanceTestCase
from edx.analytics.tasks.url import url_path_join


log = logging.getLogger(__name__)


class EnrollmentTrendsAcceptanceTest(AcceptanceTestCase):

    INPUT_FILE = 'enrollment_trends_tracking.log'
    DATE_INTERVAL = Custom.parse('2014-08-01-2014-08-06')

    def test_enrollment_trends(self):
        self.upload_tracking_log(self.INPUT_FILE, datetime.date(2014, 8, 1))

        blacklist_date = '2014-08-29'
        blacklist_url = url_path_join(
            self.warehouse_path, 'course_enrollment_blacklist', 'dt=' + blacklist_date, 'blacklist.tsv')
        with S3Target(blacklist_url).open('w') as s3_file:
            s3_file.write('edX/Open_DemoX/edx_demo_course3')

        config_override = {
            'enrollments': {
                'blacklist_date': blacklist_date,
            }
        }

        self.task.launch([
            'EnrollmentDailyTask',
            '--credentials', self.export_db.credentials_file_url,
            '--source', self.test_src,
            '--interval', self.DATE_INTERVAL.to_string(),
            '--credentials', self.export_db.credentials_file_url,
            '--n-reduce-tasks', str(self.NUM_REDUCERS),
        ], config_override=config_override)

        self.validate_output()

    def validate_output(self):
        with self.export_db.cursor() as cursor:
            cursor.execute('SELECT date, course_id, count FROM course_enrollment_daily ORDER BY date, course_id ASC')
            results = cursor.fetchall()

        self.assertItemsEqual(results, [
            (datetime.date(2014, 8, 1), 'edX/Open_DemoX/edx_demo_course', 3),
            (datetime.date(2014, 8, 2), 'edX/Open_DemoX/edx_demo_course', 2),
            (datetime.date(2014, 8, 3), 'edX/Open_DemoX/edx_demo_course', 3),
            (datetime.date(2014, 8, 3), 'course-v1:edX+Open_DemoX+edx_demo_course2', 1),
            (datetime.date(2014, 8, 4), 'edX/Open_DemoX/edx_demo_course', 2),
            (datetime.date(2014, 8, 5), 'edX/Open_DemoX/edx_demo_course', 1),
            (datetime.date(2014, 8, 5), 'course-v1:edX+Open_DemoX+edx_demo_course2', 2),
        ])
