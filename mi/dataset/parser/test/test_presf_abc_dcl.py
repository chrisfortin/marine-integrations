#!/usr/bin/env python

"""
@package mi.dataset.parser.test.test_presf_abc_dcl
@file marine-integrations/mi/dataset/parser/test/test_presf_abc_dcl.py
@author Christopher Fortin
@brief Test code for a presf_abc_dcl data parser
"""

from nose.plugins.attrib import attr

from mi.core.log import get_logger ; log = get_logger()
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticleKey

from mi.dataset.test.test_parser import ParserUnitTestCase
from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.parser.presf_abc_dcl import PresfAbcDclParser, StateKey
from mi.dataset.parser.presf_abc_dcl import PresfAbcDclParserDataParticle, DataTypeKey


RESOURCE_PATH = os.path.join(Config().base_dir(), 'mi',
			     'dataset', 'driver', 'presf_abc',
			     'dcl', 'resource')


# The list of generated tests are the suggested tests, but there may
# be other tests needed to fully test your parser

@attr('UNIT', group='mi')
class PresfAbcDclParserUnitTestCase(ParserUnitTestCase):
    """
    presf_abc_dcl Parser unit test suite
    """

    def pub_callback(self, pub):
        """ Call back method to watch what comes in via the publish callback """
        self.publish_callback_value = pub

    def setUp(self):
	ParserUnitTestCase.setUp(self)
	
	self.config = {
	    DataSetDriverConfigKeys.PARTICLE_MODULE: 'mi.dataset.parser.presf_abc_dcl',
            DataSetDriverConfigKeys.PARTICLE_CLASS: 'PresfAbcDclParserDataParticle'
	    }
        # Define test data particles and their associated timestamps which will be 
        # compared with returned results

        self.file_ingested_value = None
        self.state_callback_value = None
        self.publish_callback_value = None
	
	self.particle_a = DostadParserTelemeteredDataParticle('')
	self.particle_b = DostadParserTelemeteredDataParticle('')
	self.particle_c = DostadParserTelemeteredDataParticle('')
	self.particle_d = DostadParserTelemeteredDataParticle('')
	self.particle_e = DostadParserTelemeteredDataParticle('')
	self.particle_f = DostadParserTelemeteredDataParticle('')
	

    def test_simple(self):
        """
	Read test data and pull out data particles one at a time.
	Assert that the results are those we expected.
	"""
        pass

    def test_get_many(self):
	"""
	Read test data and pull out multiple data particles at one time.
	Assert that the results are those we expected.
	"""
        pass

    def test_long_stream(self):
        """
        Test a long stream 
        """
        pass

    def test_mid_state_start(self):
        """
        Test starting the parser in a state in the middle of processing
        """
        pass

    def test_set_state(self):
        """
        Test changing to a new state after initializing the parser and 
        reading data, as if new data has been found and the state has
        changed
        """
        pass

    def test_bad_data(self):
        """
        Ensure that bad data is skipped when it exists.
        """
        pass
