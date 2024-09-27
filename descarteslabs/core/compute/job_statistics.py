# Copyright 2018-2024 Descartes Labs.

from typing import List, Tuple

from ..common.client import Attribute, Document, ListAttribute


class CpuStatistics(Document):
    total: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total CPU usage as a percentage",
    )
    time: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Number of CPU nanoseconds used",
    )
    timeseries: List[Tuple[int, int]] = ListAttribute(
        tuple,
        readonly=True,
        doc="""Timeseries of CPU usage.

        Each list element holds the cpu percentage and nanoseconds used for that 5 minute interval.
        """,
    )


class MemoryStatistics(Document):
    total_bytes: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total memory usage in bytes",
    )
    total_percentage: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total memory usage as a percentage",
    )
    peak_bytes: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Peak memory usage in bytes",
    )
    peak_percentage: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Peak memory usage as a percentage",
    )
    timeseries: List[Tuple[int, int]] = ListAttribute(
        tuple,
        readonly=True,
        doc="""Timeseries of the memory usage.

        Each list element holds the memory percentage and memory used in bytes for
        that 5 minute interval.
        """,
    )


class NetworkStatistics(Document):
    rx_bytes: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of bytes received",
    )
    tx_bytes: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of bytes transmitted",
    )
    rx_dropped: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of packets dropped on receive",
    )
    tx_dropped: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of packets dropped on transmit",
    )
    rx_errors: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of receive errors",
    )
    tx_errors: int = Attribute(
        int,
        readonly=True,
        default=0,
        doc="Total number of transmit errors",
    )


class JobStatistics(Document):
    cpu: CpuStatistics = Attribute(CpuStatistics, readonly=True)
    memory: MemoryStatistics = Attribute(MemoryStatistics, readonly=True)
    network: NetworkStatistics = Attribute(NetworkStatistics, readonly=True)
