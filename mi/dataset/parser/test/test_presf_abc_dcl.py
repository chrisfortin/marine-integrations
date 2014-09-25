#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

Files used for testing:

20140417.presf1.log
  Metadata - 1 set,  Sensor Data - 0 records

20140417.presf2.log
  Metadata - 1 set,  Tide Data - 1 record

20140417.presf2.log
  Metadata - 4 sets,  Sensor Data - 13 records

20140417.presf2.log
  Metadata - 5 sets,  Sensor Data - 5 records

20140417.presf2.log
  Metadata - 4 sets,  Sensor Data - 6 records

20140417.presf2.log
  Metadata - 1 set,  Sensor Data - 300 records

20140417.presf2.log
  Metadata - 2 sets,  Sensor Data - 200 records

20140417.presf2.log
  This file contains a boatload of invalid sensor data records.
  See metadata in file for a list of the errors.
  20 metadata records, 47 sensor data records


"""

import os
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
    PresfAbcDclParser


RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
			     'dataset', 'driver', 'presf_abc',
			     'dcl', 'resource')


MODULE_NAME = 'mi.dataset.parser.presf_abc_dcl'


# file 1: just a starting metadata block
FILE1 = '20140417.presf1.log'
# file 2: metadata and a single TIDE sample
FILE2 = '20140417.presf2.log'
# file 3:  3x(meta, tide, wave)
FILE3 = '20140417.presf3.log'
FILE4 = '20140417.presf4.log'
FILE5 = '20140417.presf5.log'
FILE6 = '20140417.presf6.log'
FILE7 = '20140417.presf7.log'
FILE8 = '20140417.presf8.log'


RECORDS_FILE6 = 300      # number of records expected
RECORDS_FILE7 = 400      # number of records expected
EXCEPTIONS_FILE8 = 47    # number of exceptions expected



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
        in_file = self.open_file(FILE3)

        parser = PresfAbcDclParser(self.config.get(DataTypeKey.PRESF_ABC_DCL_RECOVERED),
                                  None, in_file,
                                  self.state_callback, self.pub_callback,
                                  self.exception_callback)

        particles = parser.get_records(1)
        log.debug("*** test_simple Num particles %s", len(particles))

	print particles
	
        for expected in EXPECTED_FILE2:
            # Generate expected particle
            expected_particle = PresfAbcDclRecoveredTideDataParticle(expected)

            # Get record and verify.
            result = parser.get_records(1)
            self.assertEqual(result, [expected_particle])

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


