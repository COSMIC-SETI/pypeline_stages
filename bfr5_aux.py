import numpy
import pyproj

import astropy.constants as const
from astropy.coordinates import SkyCoord
from astropy.time import Time

import erfa


def degrees_process(value):
    if isinstance(value, str):
        if value.count(':') == 2:
            value = value.split(':')
            return float(value[0]) + (float(value[1]) + float(value[2])/60)/60
        return float(value)
    return float(value)

def transform_antenna_positions_xyz_to_ecef(longitude, latitude, altitude, antenna_positions):
    transformer = pyproj.Proj.from_proj(
        pyproj.Proj(proj='latlong', ellps='WGS84', datum='WGS84'),
        pyproj.Proj(proj='geocent', ellps='WGS84', datum='WGS84'),
    )
    telescopeCenterXyz = transformer.transform(
        longitude,
        latitude,
        altitude,
    )
    for i in range(antenna_positions.shape[0]):
        antenna_positions[i, :] -= telescopeCenterXyz

def _compute_ha_dec_with_astrom(astrom, radec):
    """Computes UVW antenna coordinates with respect to reference
    Args:
        radec: SkyCoord
    
    Returns:
        (ra=Hour-Angle, dec=Declination, unit='rad')
    """
    ri, di = erfa.atciq(
        radec.ra.rad, radec.dec.rad,
        0, 0, 0, 0,
        astrom
    )
    aob, zob, ha, dec, rob = erfa.atioq(
        ri, di,
        astrom
    )
    return ha, dec

def _compute_uvw(ts, source, ant_coordinates, lla, dut1=0.0):
    """Computes UVW antenna coordinates with respect to reference

    Args:
        ts: array of Times to compute the coordinates
        source: source SkyCoord
        ant_coordinates: numpy.ndarray
            Antenna ECEF coordinates. This is indexed as (antenna_number, xyz)
        lla: tuple Reference Coordinates (radians)
            Longitude, Latitude, Altitude. The antenna_coordinates must have
            this component in them.

    Returns:
        The UVW coordinates in metres of each antenna. This
        is indexed as (antenna_number, uvw)
    """

    # get valid eraASTROM instance
    astrom, eo = erfa.apco13(
        ts.jd, 0,
        dut1,
        *lla,
        0, 0,
        0, 0, 0, 0
    )
    ha_rad, dec_rad = _compute_ha_dec_with_astrom(astrom, source)
    sin_long_minus_hangle = numpy.sin(lla[0]-ha_rad)
    cos_long_minus_hangle = numpy.cos(lla[0]-ha_rad)
    sin_declination = numpy.sin(dec_rad)
    cos_declination = numpy.cos(dec_rad)

    uvws = numpy.zeros(ant_coordinates.shape, dtype=numpy.float64)
    
    for ant in range(ant_coordinates.shape[0]):
        # RotZ(long-ha) anti-clockwise
        x = cos_long_minus_hangle*ant_coordinates[ant, 0] - (-sin_long_minus_hangle)*ant_coordinates[ant, 1]
        y = (-sin_long_minus_hangle)*ant_coordinates[ant, 0] + cos_long_minus_hangle*ant_coordinates[ant, 1]
        z = ant_coordinates[ant, 2]
        
        # RotY(declination) clockwise
        x_ = x
        x = cos_declination*x_ + sin_declination*z
        z = -sin_declination*x_ + cos_declination*z
        
        # Permute (WUV) to (UVW)
        uvws[ant, 0] = y
        uvws[ant, 1] = z
        uvws[ant, 2] = x
        
    return uvws

def _create_delay_phasors(delay, frequencies):
    return -1.0j*2.0*numpy.pi*delay*frequencies


def _get_fringe_rate(delay, fringeFrequency):
    return -1.0j*2.0*numpy.pi*delay*fringeFrequency


def phasors(
    antennaPositions: numpy.ndarray, # [Antenna, XYZ]
    boresightCoordinate: SkyCoord, # ra-dec
    beamCoordinates: 'list[SkyCoord]', #  ra-dec
    times: numpy.ndarray, # [unix]
    frequencies: numpy.ndarray, # [channel-frequencies] Hz
    calibrationCoefficients: numpy.ndarray, # [Frequency-channel, Polarization, Antenna]
    lla: tuple, # Longitude, Latitude, Altitude (radians)
    referenceAntennaIndex: int = 0,
):
    """
    Return
    ------
        phasors (B, A, F, T, P), delays_ns (T, A, B)

    """

    assert frequencies.shape[0] % calibrationCoefficients.shape[0] == 0, f"Calibration Coefficients' Frequency axis is not a factor of frequencies: {calibrationCoefficients.shape[0]} vs {frequencies.shape[0]}."

    phasorDims = (
        beamCoordinates.shape[0],
        antennaPositions.shape[0],
        frequencies.shape[0],
        times.shape[0],
        calibrationCoefficients.shape[1]
    )
    calibrationCoeffFreqRatio = frequencies.shape[0] // calibrationCoefficients.shape[0]

    phasors = numpy.zeros(phasorDims, dtype=numpy.complex128)
    
    delays_ns = numpy.zeros(
        (
            times.shape[0],
            beamCoordinates.shape[0],
            antennaPositions.shape[0],
        ),
        dtype=numpy.float64
    )

    for t, tval in enumerate(times):
        ts = Time(tval, format='unix')
        boresightUvw = _compute_uvw(
            ts,
            boresightCoordinate, 
            antennaPositions,
            lla
        )
        boresightUvw -= boresightUvw[referenceAntennaIndex:referenceAntennaIndex+1, :]
        for b in range(phasorDims[0]):
            # These UVWs are centred at the reference antenna, 
            # i.e. UVW_irefant = [0, 0, 0]
            beamUvw = _compute_uvw( # [Antenna, UVW]
                ts,
                beamCoordinates[b], 
                antennaPositions,
                lla
            )
            beamUvw -= beamUvw[referenceAntennaIndex:referenceAntennaIndex+1, :]

            delays_ns[t, b, :] = (beamUvw[:,2] - boresightUvw[:,2]) * (1e9 / const.c.value)
            for a, delay in enumerate(delays_ns[t, b, :]):
                delay_factors = _create_delay_phasors(
                    delay,
                    frequencies - frequencies[0]
                )
                fringe_factor = _get_fringe_rate(
                    delay,
                    frequencies[0]
                )

                phasor = numpy.exp(delay_factors+fringe_factor)
                for p in range(phasorDims[-1]):
                    for c in range(calibrationCoeffFreqRatio):
                        fine_slice = range(c, frequencies.shape[0], calibrationCoeffFreqRatio)
                        phasors[b, a, fine_slice, t, p] = phasor[fine_slice] * calibrationCoefficients[:, p, a]
    return phasors, delays_ns