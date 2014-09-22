#!/usr/bin/env python

"""
@package mi.dataset.parser.presf_abc_dcl
@file marine-integrations/mi/dataset/parser/presf_abc_dcl.py
@author Christopher Fortin
@brief Parser for the presf_abc_dcl dataset driver
Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import copy
import re
import ntplib

from mi.core.log import get_logger ; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import DataParticle, DataParticleKey
from mi.core.exceptions import SampleException, DatasetParserException, UnexpectedDataException

from mi.dataset.dataset_parser import BufferLoadingParser

class DataTypeKey(BaseEnum):
    """
    These are the possible harvester/parser pairs for this driver
    """
    PREFS_ABS_DCL_TELEMETERED = 'presf_abc_dcl_telemetered'
    PRESF_ABC_DCL_RECOVERED = 'presf_abc_dcl_recovered'

# a regex to match data/time in YYYY/MM/DD HH:MM:SS.sss format
DATE_TIME_REGEX = r'\A\d{4}/\d{2}/\d{2} \d(2):\d(2):\d(2).\d(3)'

# match a state message
STATE_REGEX = DATE_TIME_REGEX + '\[ \]:'
STATE_MATCHER = re.compile(STATE_REGEX)
# match a single line TIDE record
TIDE_REGEX = ''
TIDE_MATCHER = re.compile(TIDE_REGEX)
# match the single start line of a wave record
WAVE_START_REGEX = ''
WAVE_START_MATCHER = re.compile(WAVE_START_REGEX)
# match a wave ptfreq record
WAVE_PTFREQ_REGEX = ''
WAVE_PTFREQ_MATCHER = re.compile(WAVE_START_REGEX)
# match a wave continuation line
WAVE_CONT_REGEX = ''
WAVE_CONT_MATCHER = re.compile(WAVE_CONT_REGEX)

class DataParticleType(BaseEnum):
    TIDE_TELEMETERED = 'presf_abc_dcl_tide_measurement'
    TIDE_RECOVERED = 'presf_abc_dcl_tide_measurement_recovered'
    WAVE_TELEMETERED = 'presf_abc_dcl_wave_burst'
    WAVE_RECOVERED = 'presf_abc_dcl_wave_burst_recovered'

class PresfAbcDclParserDataParticleKey(BaseEnum):
    # time particles
    TIME = 'time'
    DCL_CONTROLLER_TIMESTAMP = 'dcl_controller_timestamp'
    DATE_TIME_STRING = 'date_time_string'
    ABSOLUTE_PRESSURE = 'absolute_pressure'
    PRESSURE_TEMP = 'pressure_temp'
    SEAWATER_TEMPERATURE = 'seawater_temperature'
    # wave particles, reusing TIME, DATE_TIME_STRING from above
    DCL_CONTROLLER_START_TIMESTAMP = 'dcl_controller_start_timestamp'
    DCL_CONTROLLER_END_TIMESTAMP = 'dcl_controller_end_timestamp'
    PTEMP_FREQUENCY = 'ptemp_frequency'
    ABSOLUTE_PRESSURE_BURST = 'absolute_pressure_burst'


class StateKey(BaseEnum):
    POSITION='position' # holds the file position

class PresfAbcDclParserDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl data set
    """

    _data_particle_type = DataParticleType.SAMPLE
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        pass

class PresfAbcDclParser(BufferLoadingParser):

    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to. 
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")
        self._timestamp = state_obj[StateKey.TIMESTAMP]
        self._state = state_obj
        self._read_state = state_obj

    def _increment_state(self, increment):
        """
        Increment the parser state
        @param timestamp The timestamp completed up to that position
        """
        self._read_state[StateKey.POSITION] += increment

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """            
        result_particles = []
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
        
        print chunk
        
        self.handle_non_data(non_data, non_end, start)

        while (chunk != None):
            data_match = DATA_MATCHER.match(chunk)
            if data_match:
                # particle-ize the data block received, return the record
                sample = self._extract_sample(self._particle_class, DATA_MATCHER, chunk, self._timestamp)
                if sample:
                    # create particle
                    log.trace("Extracting sample chunk %s with read_state: %s", chunk, self._read_state)
                    self._increment_state(len(chunk))
                    result_particles.append((sample, copy.copy(self._read_state)))

            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
            self.handle_non_data(non_data, non_end, start)

        return result_particles

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # if non-data is expected, handle it here, otherwise it is an error
        if non_data is not None and non_end <= start:
            # if this non-data is an error, send an UnexpectedDataException and increment the state
            self._increment_state(len(non_data))
            # if non-data is a fatal error, directly call the exception, if it is not use the _exception_callback
            self._exception_callback(UnexpectedDataException("Found %d bytes of un-expected non-data %s" % (len(non_data), non_data)))
