#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

"""

import os
import ntplib

from nose.plugins.attrib import attr

from mi.idk.config import Config

from mi.core.log import get_logger ; log = get_logger()
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticleKey

from mi.dataset.test.test_parser import ParserUnitTestCase

from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.parser.presf_abc_dcl import \
    PresfAbcDclRecoveredTideDataParticle, \
    PresfAbcDclTelemeteredTideDataParticle, \
    PresfAbcDclRecoveredWaveDataParticle, \
    PresfAbcDclTelemeteredWaveDataParticle, \
    StateKey, \
    DataTypeKey, \
    TIDE_PARTICLE_CLASS_KEY, \
    WAVE_PARTICLE_CLASS_KEY, \
    TIDE_MATCHER, \
    PresfAbcDclParser


RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
			     'dataset', 'driver', 'presf_abc',
			     'dcl', 'resource')


MODULE_NAME = 'mi.dataset.parser.presf_abc_dcl'




# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class PresfAbcDclParserUnitTestCase(ParserUnitTestCase):
    """
    presf_abc_dcl Parser unit test suite
    """
    def state_callback(self, state, file_ingested):
        """ Call back method to watch what comes in via the position callback """
        self.state_callback_value = state
        self.file_ingested_value = file_ingested

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def exception_callback(self, exception):
        """ Callback method to watch what comes in via the exception callback """
        self.exception_callback_value = exception
        self.count += 1

    def open_file(self, filename):
        file = open(os.path.join(RESOURCE_PATH, filename), mode='r')
        return file

    def setUp(self):
        ParserUnitTestCase.setUp(self)

        self.config = {
            DataTypeKey.PREFS_ABS_DCL_TELEMETERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    TIDE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredTideDataParticle,
                    WAVE_PARTICLE_CLASS_KEY: PresfAbcDclTelemeteredWaveDataParticle,
                }
            },
            DataTypeKey.PRESF_ABC_DCL_RECOVERED: {
                DataSetDriverConfigKeys.PARTICLE_MODULE: MODULE_NAME,
                DataSetDriverConfigKeys.PARTICLE_CLASS: None,
                DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT: {
                    TIDE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredTideDataParticle,
                    WAVE_PARTICLE_CLASS_KEY: PresfAbcDclRecoveredWaveDataParticle,
                }
            },
	}


	# Telemetered test data
	
        posix_time = int('51EC763C', 16)
        self._timestamp1 = ntplib.system_to_ntp_time(float(posix_time))
        self.particle_t1 = PresfAbcDclTelemeteredTideDataParticle( \
            '2014/04/17 17:59:11.323 tide: start time = 17 Apr 2014 17:00:00, p = 14.6612, pt = 11.380, t = 11.5248\r\n', \
            internal_timestamp=self._timestamp1)


        posix_time = int('51EC763C', 16)
        self._timestamp2 = ntplib.system_to_ntp_time(float(posix_time))
        self.particle_w1 = PresfAbcDclTelemeteredWaveDataParticle( \
            '2014/04/17 17:59:12.334 wave: start time = 17 Apr 2014 17:59:17\r\n' \
	    '2014/04/17 17:59:12.362 wave: ptfreq = 171226.968\r\n' \
	    '2014/04/17 17:59:12.878   14.6498\r\n' \
	    '2014/04/17 17:59:13.128   14.6482\r\n' \
	    '2014/04/17 17:59:13.378   14.6482\r\n' \
	    '2014/04/17 17:59:13.628   14.6482\r\n' \
	    '2014/04/17 17:59:13.878   14.6511\r\n' \
	    '2014/04/17 17:59:14.128   14.6511\r\n' \
	    '2014/04/17 17:59:14.378   14.6566\r\n' \
	    '2014/04/17 17:59:14.628   14.6538\r\n' \
	    '2014/04/17 17:59:14.878   14.6566\r\n' \
	    '2014/04/17 17:59:15.128   14.6566\r\n' \
	    '2014/04/17 17:59:15.378   14.6544\r\n' \
	    '2014/04/17 17:59:15.628   14.6544\r\n' \
	    '2014/04/17 17:59:15.878   14.6490\r\n' \
	    '2014/04/17 17:59:16.128   14.6462\r\n' \
	    '2014/04/17 17:59:16.378   14.6407\r\n' \
	    '2014/04/17 17:59:16.628   14.6352\r\n' \
	    '2014/04/17 17:59:16.878   14.6297\r\n' \
	    '2014/04/17 17:59:17.128   14.6270\r\n' \
	    '2014/04/17 17:59:17.378   14.6270\r\n' \
	    '2014/04/17 17:59:17.628   14.6324\r\n' \
	    '2014/04/17 17:59:17.666 wave: end burst\r\n', \
            internal_timestamp=self._timestamp2)





        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
        self.exception_callback_value = None
        self.count = 0

        self.maxDiff = None


    def test_simple(self):
        """
        Read data from a file and pull out data particles
        one at a time. Verify that the results are those we expected.
        """
        log.debug('===== START TEST SIMPLE RECOVERED =====')
        in_file = self.open_file('20140417.presf3.log')

        parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                  None, in_file,
                                  self.state_callback, self.pub_callback,
                                  self.exception_callback)

        particles = parser.get_records(1)
	print particles
	
	# file has one tide particle and one wave particle
        result = parser.get_records(1)
        self.assertEqual(result, [self.particle_t1])
	
        result = parser.get_records(1)
        self.assertEqual(result, [self.particle_w1])

        self.assertEqual(self.rec_exception_callback_value, None)
        in_file.close()

        log.debug('===== START TEST SIMPLE TELEMETERED =====')
        in_file = self.open_file(FILE3)
        parser = self.create_tel_parser(in_file)

        for expected in EXPECTED_FILE3:

            # Generate expected particle
            expected_particle = PresfAbcDclTelemeteredTideDataParticle(expected)

            # Get record and verify.
            result = parser.get_records(1)
            self.assertEqual(result, [expected_particle])

        self.assertEqual(self.tel_exception_callback_value, None)
        in_file.close()

        log.debug('===== END TEST SIMPLE =====')


