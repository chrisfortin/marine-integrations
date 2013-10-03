#!/usr/bin/env python

"""
@package mi.dataset.parser.ctdmo 
@file mi/dataset/parser/ctdmo.py
@author Emily Hzhn
@brief A CTDMO-specific data set agent parser
"""

__author__ = 'Emily Hahn'
__license__ = 'Apache 2.0'

import binascii
import array
import string
import re
import time
import ntplib
from mi.core.log import get_logger ; log = get_logger()

from mi.dataset.parser.mflm import MflmParser
from mi.core.common import BaseEnum
from mi.core.exceptions import SampleException
from mi.core.instrument.data_particle import DataParticle, DataParticleKey

class DataParticleType(BaseEnum):
    SAMPLE = 'nose_ctd_external'
    
class CtdmoParserDataParticleKey(BaseEnum):
    TEMPERATURE = "temperature"
    CONDUCTIVITY = "conductivity"
    PRESSURE = "pressure"

# the [\x16-\x40] is because we need more than just \x0d to correctly
# identify the split between samples, the data might have \x0d in it also,
# so since this value has to do with the year, x16 = august 2011 to
# x40 = july 2034
DATA_REGEX = b'[\x00-\xFF]{8}([\x00-\xFF]{3}[\x16-\x40]{1})\x0d'
DATA_MATCHER = re.compile(DATA_REGEX)

class CtdmoParserDataParticle(DataParticle):
    """
    Class for parsing data from the CTDMO instrument on a MSFM platform node
    """
    
    _data_particle_type = DataParticleType.SAMPLE
    
    def _build_parsed_values(self):
        """
        Take something in the data format CSV delimited values and turn it into
        a particle with the appropriate tag.
        @throws SampleException If there is a problem with sample creation
        """
        
        match = DATA_MATCHER.match(self.raw_data)
        if not match:
            raise SampleException("CtdParserDataParticle: No regex match of \
                                  parsed sample data: [%s]", self.raw_data)

        try:
            # convert binary to hex ascii string
            asciihex = binascii.b2a_hex(match.group(0))
            log.debug("converting particle hex ascii %s", asciihex)
            # just convert directly from hex-ascii to int
            temp_num = int(asciihex[2:7], 16)
            temp = (temp_num / 10000) - 10
            cond_num = int(asciihex[7:12], 16)
            cond = (cond_num / 100000) - .5
            # need to swap pressure bytes
            press_byte_swap = asciihex[14:16] + asciihex[12:14]
            press_num = int(press_byte_swap, 16)
            pressure_range = .6894757 * (1000 - 14)
            press = (press_num * pressure_range / (.85 * 65536)) - (.05 * pressure_range)

        except (ValueError, TypeError, IndexError) as ex:
            raise SampleException("Error (%s) while decoding parameters in data: [%s]"
                                  % (ex, self.raw_data))

        result = [{DataParticleKey.VALUE_ID: CtdmoParserDataParticleKey.TEMPERATURE,
                   DataParticleKey.VALUE: temp},
                  {DataParticleKey.VALUE_ID: CtdmoParserDataParticleKey.CONDUCTIVITY,
                   DataParticleKey.VALUE: cond},
                  {DataParticleKey.VALUE_ID: CtdmoParserDataParticleKey.PRESSURE,
                   DataParticleKey.VALUE: press}]
        log.debug('CtdmoParserDataParticle: particle=%s', result)
        return result

    def __eq__(self, arg):
        """
        Quick equality check for testing purposes. If they have the same raw
        data and timestamp, they are the same enough for this particle
        """
        if ((self.raw_data == arg.raw_data) and \
            (self.contents[DataParticleKey.INTERNAL_TIMESTAMP] == arg.contents[DataParticleKey.INTERNAL_TIMESTAMP])):
            return True
        else:
            return False


class CtdmoParser(MflmParser):

    def __init__(self,
                 config,
                 state,
                 stream_handle,
                 state_callback,
                 publish_callback,
                 *args, **kwargs):
        super(CtdmoParser, self).__init__(config,
                                          stream_handle,
                                          state,
                                          self.sieve_function,
                                          state_callback,
                                          publish_callback,
                                          'CT',
                                          *args,
                                          **kwargs)

    @staticmethod
    def _convert_time_to_timestamp(sec_since_2000):
        """
        Converts the given string in matched format into an NTP timestamp.
        @param ts_str The timestamp string in the format "mm/dd/yyyy hh:mm:ss"
        @retval The NTP4 timestamp
        """
        # convert from epoch in 2000 to epoch in 1970 for time
        sec_since_1970 = sec_since_2000 + (31557600*30)
        systime = time.localtime(sec_since_1970)
        ntptime = ntplib.system_to_ntp_time(time.mktime(systime))
        log.debug("Converted sys time \"%s\" into ntp %s", systime, ntptime) 
        return ntptime

    def parse_chunks(self):
        """
        Parse out any pending data chunks in the chunker. If
        it is a valid data piece, build a particle, update the position and
        timestamp. Go until the chunker has no more valid data.
        @retval a list of tuples with sample particles encountered in this
            parsing, plus the state. An empty list of nothing was parsed.
        """            
        result_particles = []
        (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        # sieve looks for timestamp, update and increment position
        while (chunk != None):
            log.debug("checking chunk %s", chunk)
            for data_match in DATA_MATCHER.finditer(chunk):
                log.debug("found sample match %s", data_match.group(0))
                # the timestamp is part of the data, pull out the time stamp
                # convert from binary to hex string
                asciihextime = binascii.b2a_hex(data_match.group(1))
                # reverse byte order in time hex string
                timehex_reverse = asciihextime[6:8] + asciihextime[4:6] + asciihextime[2:4] + asciihextime[0:2]
                # time is in seconds since Jan 1 2000, convert to timestamp
                log.trace("time in hex:%s, in seconds:%d", timehex_reverse, int(timehex_reverse, 16))
                self._timestamp = self._convert_time_to_timestamp(int(timehex_reverse, 16))
                # particle-ize the data block received, return the record
                sample = self._extract_sample(CtdmoParserDataParticle, DATA_MATCHER, data_match.group(0), self._timestamp)
                if sample:
                    # create particle
                    result_particles.append(sample)
            (timestamp, chunk, start, end) = self._chunker.get_next_data_with_index()

        return result_particles

