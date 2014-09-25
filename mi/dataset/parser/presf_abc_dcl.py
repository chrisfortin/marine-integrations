#!/usr/bin/env python

"""
@package mi.dataset.parser.presf_abc_dcl
@file marine-integrations/mi/dataset/parser/presf_abc_dcl.py
@author Christopher Fortin
@brief Parser for the presf_abc_dcl dataset driver

This file contains code for the presf_abc_dcl parsers and code to produce data particles.
For telemetered data, there is one parser which produces two types of data particles.
For recovered data, there is one parser which produces two types of data particles.
The input file formats are the same for both recovered and telemetered.
Only the names of the output particle streams are different.

The input file is ASCII and contains 3 types of records.
Two of the record types are separated by a newline.
The third type, a data burst, is a continuing list of data values,
one per line, that continues for an arbitrary period, ending with
an explicit 'end' line
All lines start with a timestamp.
Metadata records: timestamp [text] more text newline.
Tide Data records: timestamp sensor_data newline.
Wave Data records: timestamp sensor_data newline.
Wave Burst records: timestamp sensor_data newline.
Wave Burst End record: timestamp 'wave: end burst'
Only sensor data records produce particles if properly formed.
Mal-formed sensor data records and all metadata records produce no particles.


Release notes:

Initial Release
"""

__author__ = 'Christopher Fortin'
__license__ = 'Apache 2.0'

import calendar
import copy
import re
from functools import partial

from mi.core.instrument.chunker import \
    StringChunker

from mi.core.log import get_logger ; log = get_logger()
from mi.core.common import BaseEnum
from mi.core.instrument.data_particle import \
    DataParticle, \
    DataParticleKey, \
    DataParticleValue

from mi.core.exceptions import \
    RecoverableSampleException

from mi.dataset.dataset_driver import DataSetDriverConfigKeys
from mi.dataset.dataset_parser import BufferLoadingParser


# Basic patterns
ANY_CHARS = r'.*'          # Any characters excluding a newline
NEW_LINE = r'(?:\r\n|\n)'  # any type of new line
UINT = r'(\d*)'            # unsigned integer as a group
FLOAT = r'([1-9][0-9]*\.?[0-9]*)'
WHITESPACE = r'(\s*)'
COMMA = ','
TAB = '\t'
START_GROUP = '('
END_GROUP = ')'

# Timestamp at the start of each record: YYYY/MM/DD HH:MM:SS.mmm
# Metadata fields:  [text] more text
# Sensor data has tab-delimited fields (date, time, integers)
# All records end with one of the newlines.
DATE = r'(\d{4})/(\d{2})/(\d{2})'         # Date: YYYY/MM/DD
TIME = r'(\d{2}):(\d{2}):(\d{2})\.\d{3}'  # Time: HH:MM:SS.mmm
DATE_TIME_STR = r'(\d{2} \D{3} \d{4} \d{2}:\d{2}:\d{2})'
TIMESTAMP = START_GROUP + DATE + WHITESPACE + TIME + END_GROUP
START_METADATA = r'\['
END_METADATA = r'\]'

# All presf records are ASCII characters separated by a newline.
PRESF_RECORD_PATTERN = ANY_CHARS       # Any number of ASCII characters
PRESF_RECORD_PATTERN += NEW_LINE       # separated by a new line
PRESF_RECORD_MATCHER = re.compile(PRESF_RECORD_PATTERN)

# Metadata record:
#   Timestamp [Text]MoreText newline
METADATA_PATTERN = TIMESTAMP + WHITESPACE                           # dcl controller timestamp
METADATA_PATTERN += START_METADATA                                  # Metadata record starts with '['
METADATA_PATTERN += ANY_CHARS                                       # followed by text
METADATA_PATTERN += END_METADATA                                    # followed by ']'
METADATA_PATTERN += ANY_CHARS                                       # followed by more text
METADATA_PATTERN += NEW_LINE                                        # metadata record ends newline
METADATA_MATCHER = re.compile(METADATA_PATTERN)

# match a single line TIDE record
TIDE_REGEX = TIMESTAMP + WHITESPACE                                 # dcl controller timestamp
TIDE_REGEX += 'tide:' + WHITESPACE                                  # record type
TIDE_REGEX += 'start time =' + WHITESPACE + DATE_TIME_STR + COMMA   # timestamp
TIDE_REGEX += WHITESPACE + 'p =' + WHITESPACE + FLOAT + COMMA       # pressure
TIDE_REGEX += WHITESPACE + 'pt =' + WHITESPACE + FLOAT + COMMA      # pressure temp
TIDE_REGEX += WHITESPACE + 't =' + WHITESPACE + FLOAT               # temp
TIDE_REGEX += NEW_LINE
TIDE_MATCHER = re.compile(TIDE_REGEX)

# match the single start line of a wave record
WAVE_START_REGEX = TIMESTAMP + WHITESPACE                           # dcl controller timestamp
WAVE_START_REGEX += 'wave:' + WHITESPACE                            # record type
WAVE_START_REGEX += 'start time =' + WHITESPACE + DATE_TIME_STR     # timestamp
WAVE_START_REGEX += NEW_LINE                                        # 
WAVE_START_MATCHER = re.compile(WAVE_START_REGEX)

# match a wave ptfreq record
WAVE_PTFREQ_REGEX = TIMESTAMP + WHITESPACE                          # dcl controller timestamp
WAVE_PTFREQ_REGEX += 'wave:' + WHITESPACE                           # record type
WAVE_PTFREQ_REGEX += 'ptfreq =' + WHITESPACE + FLOAT                # pressure temp
WAVE_PTFREQ_REGEX += NEW_LINE
WAVE_PTFREQ_MATCHER = re.compile(WAVE_PTFREQ_REGEX)

# match a wave continuation line
WAVE_CONT_REGEX = TIMESTAMP + WHITESPACE + FLOAT                    # dcl controller timestamp
WAVE_CONT_REGEX += NEW_LINE
WAVE_CONT_MATCHER = re.compile(WAVE_CONT_REGEX)

# match the single start line of a wave record
WAVE_END_REGEX = TIMESTAMP + WHITESPACE                             # dcl controller timestamp
WAVE_END_REGEX += 'wave: end burst'                                 # record type
WAVE_END_REGEX += NEW_LINE
WAVE_END_MATCHER = re.compile(WAVE_END_REGEX)

# TIDE_DATA_MATCHER produces the following groups:
TIDE_GROUP_DCL_TIMESTAMP = 1
TIDE_GROUP_YEAR = 2
TIDE_GROUP_MONTH = 3
TIDE_GROUP_DAY = 4
TIDE_GROUP_HOUR = 6
TIDE_GROUP_MINUTE = 7
TIDE_GROUP_SECOND = 8
TIDE_GROUP_DATA_TIME_STRING = 12
TIDE_GROUP_ABSOLUTE_PRESSURE = 15
TIDE_GROUP_PRESSURE_TEMPERATURE = 18
TIDE_GROUP_SEAWATER_TEMPERATURE = 21

# WAVE_DATA_MATCHER produces the following groups:
WAVE_START_GROUP_DCL_TIMESTAMP = 1
WAVE_START_GROUP_YEAR = 2
WAVE_START_GROUP_MONTH = 3
WAVE_START_GROUP_DAY = 4
WAVE_START_GROUP_HOUR = 6
WAVE_START_GROUP_MINUTE = 7
WAVE_START_GROUP_SECOND = 8
WAVE_START_GROUP_DATE_TIME_STRING = 12

# WAVE_PTFREQ_MATCHER produces the following groups:
WAVE_PTFREQ_GROUP_PTEMP_FREQUENCY = 12

# CONT produces the following groups:
WAVE_CONT_GROUP_ABSOLUTE_PRESSURE = 10

# WAVE_END_MATCHER produces the following groups:
WAVE_END_GROUP_DCL_TIMESTAMP = 1

# This table is used in the generation of the tide data particle.
# Column 1 - particle parameter name
# Column 2 - group number (index into raw_data)
# Column 3 - data encoding function (conversion required - int, float, etc)
TIDE_PARTICLE_MAP = [
    ('dcl_controller_timestamp',    TIDE_GROUP_DCL_TIMESTAMP,            str),
    ('date_time_string',            TIDE_GROUP_DATA_TIME_STRING,         str),
    ('absolute_pressure',           TIDE_GROUP_ABSOLUTE_PRESSURE,        float),
    ('pressure_temperature',        TIDE_GROUP_PRESSURE_TEMPERATURE,     float),
    ('seawater_temperature',        TIDE_GROUP_SEAWATER_TEMPERATURE,     float)
]

class PresfAbcDclWaveParticleKey(BaseEnum):
    DCL_CONTROLLER_START_TIMESTAMP = 'dcl_controller_start_timestamp'
    DCL_CONTROLLER_END_TIMESTAMP = 'dcl_controller_end_timestamp'
    DATE_TIME_STRING = 'date_time_string'
    PTEMP_FREQUENCY = 'ptemp_frequency'
    ABSOLUTE_PRESSURE_BURST = 'absolute_pressure_burst'

# The following two keys are keys to be used with the PARTICLE_CLASSES_DICT
# The key for the metadata particle class
TIDE_PARTICLE_CLASS_KEY = 'tide_particle_class'
# The key for the data particle class
WAVE_PARTICLE_CLASS_KEY = 'wave_particle_class'

class DataParticleType(BaseEnum):
    TIDE_TELEMETERED = 'presf_abc_dcl_tide_measurement'
    TIDE_RECOVERED = 'presf_abc_dcl_tide_measurement_recovered'
    WAVE_TELEMETERED = 'presf_abc_dcl_wave_burst'
    WAVE_RECOVERED = 'presf_abc_dcl_wave_burst_recovered'

class StateKey(BaseEnum):
    POSITION = 'position'                                             # holds the file position

class DataTypeKey(BaseEnum):
    """
    These are the possible harvester/parser pairs for this driver
    """
    PREFS_ABS_DCL_TELEMETERED = 'presf_abc_dcl_telemetered'
    PRESF_ABC_DCL_RECOVERED = 'presf_abc_dcl_recovered'

class PresfAbcDclParserTideDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl tide data set
    """

    def __init__(self, raw_data,
        port_timestamp=None,
        internal_timestamp=None,
        preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
        quality_flag=DataParticleValue.OK,
        new_sequence=None):

        super(PresfAbcDclParserTideDataParticle, self).__init__(raw_data,
                                                          port_timestamp,
                                                          internal_timestamp,
                                                          preferred_timestamp,
                                                          quality_flag,
                                                          new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        
        tide_match = TIDE_MATCHER.match(raw_data)
       
        timestamp = (
            int(tide_match.group(TIDE_GROUP_YEAR)),
            int(tide_match.group(TIDE_GROUP_MONTH)),
            int(tide_match.group(TIDE_GROUP_DAY)),
            int(tide_match.group(TIDE_GROUP_HOUR)),
            int(tide_match.group(TIDE_GROUP_MINUTE)),
            int(tide_match.group(TIDE_GROUP_SECOND)),
            0, 0, 0)
        
        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        tide_match = TIDE_MATCHER.match(self.raw_data)
        
        return [self._encode_value(name, tide_match.group(group), function)
            for name, group, function in TIDE_PARTICLE_MAP]


class PresfAbcDclParserWaveDataParticle(DataParticle):
    """
    Class for parsing data from the presf_abc_dcl wave data set
    """

    def __init__(self, raw_data,
        port_timestamp=None,
        internal_timestamp=None,
        preferred_timestamp=DataParticleKey.PORT_TIMESTAMP,
        quality_flag=DataParticleValue.OK,
        new_sequence=None):

        super(PresfAbcDclParserWaveDataParticle, self).__init__(raw_data,
                                                          port_timestamp,
                                                          internal_timestamp,
                                                          preferred_timestamp,
                                                          quality_flag,
                                                          new_sequence)

        # The particle timestamp is the DCL Controller timestamp.
        # The individual fields have already been extracted by the parser.
        
        wave_match = WAVE_START_MATCHER.match(raw_data)
       
        timestamp = (
            int(wave_match.group(WAVE_START_GROUP_YEAR)),
            int(wave_match.group(WAVE_START_GROUP_MONTH)),
            int(wave_match.group(WAVE_START_GROUP_DAY)),
            int(wave_match.group(WAVE_START_GROUP_HOUR)),
            int(wave_match.group(WAVE_START_GROUP_MINUTE)),
            int(wave_match.group(WAVE_START_GROUP_SECOND)),
            0, 0, 0)
        
        elapsed_seconds = calendar.timegm(timestamp)
        self.set_internal_timestamp(unix_time=elapsed_seconds)
    
    def _build_parsed_values(self):
        """
        Take something in the data format and turn it into
        an array of dictionaries defining the data in the particle
        with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """

        result = []
        absolute_pressure_burst = []
        
        # NOTE: the format of the wave record was validated in the sieve, so
        # here we assume records are in the correct order

        data_index = 0
        raw_data_len = len(self.raw_data)

        while data_index < raw_data_len:
            record_match = PRESF_RECORD_MATCHER.match(self.raw_data[data_index:])
            if record_match:
                # wave records span several lines, wstart begins one
                test_wstart = WAVE_START_MATCHER.match(record_match.group(0))
                if test_wstart != None:
                    wave_match = WAVE_START_MATCHER.match(self.raw_data)
                    dcl_controller_start_timestamp = wave_match.group(WAVE_START_GROUP_DCL_TIMESTAMP)
                    date_time_string = wave_match.group(WAVE_START_GROUP_DATE_TIME_STRING)
                    data_index += len(test_wstart.group())
                    continue

                test_ptfreq = WAVE_PTFREQ_MATCHER.match(record_match.group(0))
                if test_ptfreq != None: 
                    ptemp_frequency = test_ptfreq.group(WAVE_PTFREQ_GROUP_PTEMP_FREQUENCY)
                    data_index += len(test_ptfreq.group())
                    continue

                test_wcont = WAVE_CONT_MATCHER.match(record_match.group(0))
                if test_wcont != None:                    
                    absolute_pressure_burst.append(test_wcont.group(WAVE_CONT_GROUP_ABSOLUTE_PRESSURE))
                    data_index += len(test_wcont.group())
                    continue

                test_wend = WAVE_END_MATCHER.match(record_match.group(0))
                if test_wend != None:                        
                    dcl_controller_end_timestamp = test_wend.group(WAVE_END_GROUP_DCL_TIMESTAMP)
                    data_index += len(test_wend.group())
                    continue
    
            else:
                # received a wave start while already parsing a wave record
                log.warn("Exception when building parsed values")
                raise RecoverableSampleException(
                    "Error while decoding parameters in data: [%s]"
                    % (raw_data))

                break
        
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DCL_CONTROLLER_START_TIMESTAMP,
                        dcl_controller_start_timestamp, str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DCL_CONTROLLER_END_TIMESTAMP,
                        dcl_controller_end_timestamp, str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.DATE_TIME_STRING,
                        date_time_string, str))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.PTEMP_FREQUENCY,
                        ptemp_frequency, float))
        result.append(self._encode_value(PresfAbcDclWaveParticleKey.ABSOLUTE_PRESSURE_BURST,
                        absolute_pressure_burst, list))

        return result     

class PresfAbcDclRecoveredTideDataParticle(PresfAbcDclParserTideDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.TIDE_RECOVERED


class PresfAbcDclTelemeteredTideDataParticle(PresfAbcDclParserTideDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.TIDE_TELEMETERED


class PresfAbcDclRecoveredWaveDataParticle(PresfAbcDclParserWaveDataParticle):
    """
    Class for generating Offset Data Particles from Recovered data.
    """
    _data_particle_type = DataParticleType.WAVE_RECOVERED


class PresfAbcDclTelemeteredWaveDataParticle(PresfAbcDclParserWaveDataParticle):
    """
    Class for generating Offset Data Particles from Telemetered data.
    """
    _data_particle_type = DataParticleType.WAVE_TELEMETERED


class PresfAbcDclParser(BufferLoadingParser):
    """
    """
    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 exception_callback,
                 *args, **kwargs):

        super(PresfAbcDclParser, self).__init__(config,
                                          stream_handle,
                                          state,
                                          self.sieve_function,
                                          state_callback,
                                          publish_callback,
                                          exception_callback,
                                          *args,
                                          **kwargs)

        self.input_file = stream_handle
        
       # Obtain the particle classes dictionary from the config data
        if DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT in config:
            particle_classes_dict = config.get(DataSetDriverConfigKeys.PARTICLE_CLASSES_DICT)
            # Set the metadata and data particle classes to be used later
            if TIDE_PARTICLE_CLASS_KEY in particle_classes_dict and \
            WAVE_PARTICLE_CLASS_KEY in particle_classes_dict:
                self._wave_particle_class = particle_classes_dict.get(WAVE_PARTICLE_CLASS_KEY)
                self._tide_particle_class = particle_classes_dict.get(TIDE_PARTICLE_CLASS_KEY)
            else:
                log.warning(
                    'Configuration missing metadata or data particle class key in particle classes dict')
                raise ConfigurationException(
                    'Configuration missing metadata or data particle class key in particle classes dict')
 

    def handle_non_data(self, non_data, non_end, start):
        """
        Handle any non-data that is found in the file
        """
        # Handle non-data here.
        # Increment the position within the file.
        # Use the _exception_callback.
        if non_data is not None and non_end <= start:
            test_meta = METADATA_MATCHER.match(non_data)
            if test_meta == None:
                self._exception_callback(RecoverableSampleException(
                    "Found %d bytes of un-expected non-data %s" %
                    (len(non_data), non_data)))


    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker.
        If it is valid data, build a particle.
        Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state.
        """

        # Initialize the result particles list we will return
        result_particles = []

        # Retrieve the next non data chunk
        (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)
 
        # Retrieve the next data chunk
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)
 
        # Process the non data
        self.handle_non_data(non_data, non_end, start)
                      
        # While the data chunk is not None, process the data chunk
        while chunk is not None:

            test_meta = METADATA_MATCHER.match(chunk)
            test_wstart = WAVE_START_MATCHER.match(chunk)
            test_tide = TIDE_MATCHER.match(chunk)
    
            if test_meta != None:
                # we don't return meta records
                log.debug("Matched a meta record")        
            
            if test_wstart != None:
                # Extract the data record particle
                log.debug('Wave data record')
                data_particle = self._extract_sample(self._wave_particle_class,
                                                     None,
                                                     chunk,
                                                     None)
                result_particles.append(data_particle)
               
            if test_tide != None:
                # Extract the data record particle
                log.debug('Tide data record')
                data_particle = self._extract_sample(self._tide_particle_class,
                                                     None,
                                                     chunk,
                                                     None)
                result_particles.append(data_particle)

            # Retrieve the next non data chunk
            (nd_timestamp, non_data, non_start, non_end) = self._chunker.get_next_non_data_with_index(clean=False)

            # Retrieve the next data chunk
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index(clean=True)

            # Process the non data
            self.handle_non_data(non_data, non_end, start)

        return result_particles
        
        
    def set_state(self, state_obj):
        """
        Set the value of the state object for this parser
        @param state_obj The object to set the state to.
        @throws DatasetParserException if there is a bad state structure
        """
        if not isinstance(state_obj, dict):
            raise DatasetParserException("Invalid state structure")

        if not (StateKey.POSITION in state_obj):
            raise DatasetParserException('%s missing in state keys' %
                                         StateKey.POSITION)

        self._record_buffer = []
        self.input_file.seek(state_obj[StateKey.POSITION])


    def sieve_function(self, raw_data):
        """
        Sort through the raw data to identify new blocks of data that need processing.
        This is needed instead of a regex because blocks are identified by position
        in this binary file.
        """
        
        # enums used to track wave record parsing
        WAVE_RECORD_NOTINPROCESS = 0
        WAVE_RECORD_STARTED      = 1
        WAVE_RECORD_PTFREQ       = 2
        WAVE_RECORD_CONT         = 3   
        
        data_index = 0
        return_list = []
        raw_data_len = len(raw_data)
        
        # a var to track state while processing multiline wave records
        wave_record_inprocess = WAVE_RECORD_NOTINPROCESS
        
        while data_index < raw_data_len:
            # Do we have a newline delimited field left?
            record_match = PRESF_RECORD_MATCHER.match(raw_data[data_index:])
            if record_match:
                # check for a metadata line ( a single line )
                test_meta = METADATA_MATCHER.match(record_match.group(0))
                if test_meta != None:
                    #return_list.append((data_index, data_index + len(test_meta.group())))
                    data_index += len(test_meta.group())
                    continue
                
                # a tide message, also a single line
                test_tide = TIDE_MATCHER.match(record_match.group(0))
                if test_tide != None:
                    return_list.append((data_index, data_index + len(test_tide.group())))
                    data_index += len(test_tide.group())
                    continue
                
                # wave records span several lines, wstart begins one
                test_wstart = WAVE_START_MATCHER.match(record_match.group(0))
                if test_wstart != None:
                    if wave_record_inprocess != WAVE_RECORD_NOTINPROCESS:
                        # received a wave start while already parsing a wave record
                        log.warn("Exception when building parsed values")
                        raise RecoverableSampleException(
                            "Error while decoding parameters in data: [%s]"
                            % (raw_data))

                    wave_record_inprocess = WAVE_RECORD_STARTED
                    wave_record_start_index = data_index
                    data_index += len(test_wstart.group())
                    continue

                test_ptfreq = WAVE_PTFREQ_MATCHER.match(record_match.group(0))
                if test_ptfreq != None:
                    if wave_record_inprocess != WAVE_RECORD_STARTED:
                        # received a wave start while already parsing a wave record
                        log.warn("Exception when building parsed values")
                        raise RecoverableSampleException(
                            "Error while decoding parameters in data: [%s]"
                            % (raw_data))

                    wave_record_inprocess = WAVE_RECORD_PTFREQ
                    data_index += len(test_ptfreq.group())
                    continue

                test_wcont = WAVE_CONT_MATCHER.match(record_match.group(0))
                if test_wcont != None:
                    if wave_record_inprocess != WAVE_RECORD_PTFREQ and \
                       wave_record_inprocess != WAVE_RECORD_CONT:
                        # received a wave start while already parsing a wave record
                        log.warn("Exception when building parsed values")
                        raise RecoverableSampleException(
                            "Error while decoding parameters in data: [%s]"
                            % (raw_data))

                    wave_record_inprocess = WAVE_RECORD_CONT
                    data_index += len(test_wcont.group())
                    continue

                test_wend = WAVE_END_MATCHER.match(record_match.group(0))
                if test_wend != None:
                    if wave_record_inprocess != WAVE_RECORD_CONT:
                        # received a wave start while already parsing a wave record
                        log.warn("Exception when building parsed values")
                        raise RecoverableSampleException(
                            "Error while decoding parameters in data: [%s]"
                            % (raw_data))

                    wave_record_inprocess = WAVE_RECORD_NOTINPROCESS
                    return_list.append((wave_record_start_index, data_index + len(test_wend.group())))
                    data_index += len(test_wend.group())
                    continue
    
                # received a wave start while already parsing a wave record
                log.warn("Exception when building parsed values")
                raise RecoverableSampleException(
                    "Error while decoding parameters in data: [%s]"
                    % (raw_data))

                break

            else:
                log.debug("not a complete record left")
                break
       
        log.debug("returning sieve list %s", return_list)
        return return_list

