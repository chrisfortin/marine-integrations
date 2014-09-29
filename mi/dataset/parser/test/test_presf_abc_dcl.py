#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser

"""

import os
import numpy
import ntplib
import yaml

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

        parser = PresfAbcDclParser(self.config.get(DataTypeKey.PREFS_ABS_DCL_TELEMETERED),
                                  None, in_file,
                                  self.state_callback, self.pub_callback,
                                  self.exception_callback)
	
	# file has one tide particle and one wave particle
        particles = parser.get_records(2)
	
        # Close the file stream as we don't need it anymore
        in_file.close()

        # Make sure we obtained 2 particles
        self.assertTrue(len(particles) == 2)

	# Obtain the expected 5 samples from a yml file
        test_data = self.get_dict_from_yml('20140417.presf3.yml')

        index = 0
        for particle in particles:
            log.info(particle.generate_dict())

            # Make sure each retrieved sample matches its expected result
            self.assert_result(test_data['data'][index], particles[index])

            index += 1

    def test_get_many(self):
        """
        Read test data and pull out multiple data particles at one time.
        Assert that the results are those we expected.
        """

        log.debug('===== START TEST MANY =====')
        in_file = self.open_file('20140105_trim.presf.log')

        parser = PresfAbcDclParser(self.config.get(DataTypeKey.PREFS_ABS_DCL_TELEMETERED),
                                  None, in_file,
                                  self.state_callback, self.pub_callback,
                                  self.exception_callback)
	
        particles = parser.get_records(20)
	
        # Close the file stream as we don't need it anymore
        in_file.close()

        # Make sure we obtained 20 particles
        self.assertTrue(len(particles) == 20)

	# Obtain the expected 5 samples from a yml file
        test_data = self.get_dict_from_yml('20140105_trim.presf.yml')

        index = 0
        for particle in particles:
            log.info(particle.generate_dict())

            # Make sure each retrieved sample matches its expected result
            self.assert_result(test_data['data'][index], particles[index])

            index += 1

    def test_long_stream(self):
        """
        Test a long stream
        """
        in_file = self.open_file('20140105.presf.log')

        parser = PresfAbcDclParser(self.config.get(DataTypeKey.PREFS_ABS_DCL_TELEMETERED),
                                  None, in_file,
                                  self.state_callback, self.pub_callback,
                                  self.exception_callback)

        particles = parser.get_records(48)
	
        # Close the file stream as we don't need it anymore
        in_file.close()

        # Make sure we obtained 20 particles
        self.assertTrue(len(particles) == 48)





    def get_dict_from_yml(self, filename):
        """
        This utility routine loads the contents of a yml file
        into a dictionary
        """

        fid = open(os.path.join(RESOURCE_PATH, filename), 'r')
        result = yaml.load(fid)
        fid.close()

        return result

    def assert_result(self, test, particle):
        """
        Suite of tests to run against each returned particle and expected
        results of the same.  The test parameter should be a dictionary
        that contains the keys to be tested in the particle
        the 'internal_timestamp' and 'position' keys are
        treated differently than others but can be verified if supplied
        """

        particle_dict = particle.generate_dict()

        log.info(particle_dict)

        #for efficiency turn the particle values list of dictionaries into a dictionary
        particle_values = {}
        for param in particle_dict.get('values'):
            particle_values[param['value_id']] = param['value']

        # compare each key in the test to the data in the particle
        for key in test:

            log.info("key: %s", key)

            test_data = test[key]

            #get the correct data to compare to the test
            if key == 'internal_timestamp':
                particle_data = particle.get_value('internal_timestamp')
                #the timestamp is in the header part of the particle

                log.info("internal_timestamp %d", particle_data)

            else:
                particle_data = particle_values.get(key)
                #others are all part of the parsed values part of the particle

            if particle_data is None:
                #generally OK to ignore index keys in the test data, verify others

                log.warning("\nWarning: assert_result ignoring test key %s, does not exist in particle", key)
            else:
                log.info("test_data %s - particle_data %s", test_data, particle_data)
                if isinstance(test_data, float):
                    # slightly different test for these values as they are floats.
                    compare = numpy.abs(test_data - particle_data) <= 1e-5
                    self.assertTrue(compare)
                else:
                    # otherwise they are all ints and should be exactly equal
                    self.assertEqual(test_data, particle_data)
