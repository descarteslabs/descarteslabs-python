import pytest

import shapely.geometry

import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import ibis.client

from ..compiler import AstDeserializer
from ...serialization.compiler import AstSerializer, make_query


@pytest.fixture(scope="function")
def schemata():
    return {
        "whosonfirst": sch.Schema(
            names=[
                "area",
                "area_square_m",
                "continent_id",
                "country",
                "country_id",
                "county_id",
                "geom",
                "geomhash",
                "locality_id",
                "parent_id",
                "placetype",
                "region_id",
                "wof_id",
                "wof_name",
            ],
            types=[
                dt.Float64(),
                dt.Float64(),
                dt.Int64(),
                dt.String(),
                dt.Int64(),
                dt.Int64(),
                dt.Geography(srid=4326),
                dt.String(),
                dt.Int64(),
                dt.Int64(),
                dt.String(),
                dt.Int64(),
                dt.Int64(),
                dt.String(),
            ],
        ),
        "ports_v1": sch.Schema(
            names=[
                "index_number",
                "region_number",
                "port_name",
                "country",
                "port_latitude",
                "port_longitude",
                "publication_number",
                "chart_number",
                "harbor_size",
                "harbor_type",
                "shelter_afforded",
                "entry_tide",
                "entry_swell",
                "entry_ice",
                "entry_other",
                "overhead_limit",
                "channel_depth",
                "anchorage_depth",
                "cargo_pier_depth",
                "cargo_oil_depth",
                "tide_range",
                "max_vessel_size",
                "good_holding_ground",
                "turning_area",
                "port_of_entry",
                "us_representative",
                "eta_message_required",
                "pilot_required",
                "pilot_available",
                "pilot_local_assist",
                "pilot_advised",
                "tug_salvage",
                "tug_assist",
                "quarantine_procedures_required",
                "quarantine_sscc_cert_required",
                "quarantine_other",
                "comms_phone",
                "comms_fax",
                "comms_radio",
                "comms_vhf",
                "comms_air",
                "comms_rail",
                "cargo_wharf",
                "cargo_anchor",
                "cargo_med_moor",
                "cargo_beach_moor",
                "cargo_ice_moor",
                "med_facility",
                "garbage_dispose",
                "degauss",
                "dirty_ballast",
                "crane_fixed",
                "crane_mobile",
                "crane_float",
                "lift_100_tons",
                "lift_50_100_tons",
                "lift_25_49_tons",
                "lift_0_24_tons",
                "services_longshore",
                "services_electrical",
                "services_steam",
                "services_nav_equip",
                "services_electrical_repair",
                "provisions",
                "water",
                "fuel_oil",
                "diesel",
                "deck_supplies",
                "eng_supplies",
                "repair_types",
                "dry_dock_types",
                "railway_types",
                "port_geom",
            ],
            types=[
                dt.String(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.Float64(),
                dt.Float64(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.Int64(),
                dt.String(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.Boolean(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.Geography(srid=4326),
            ],
        ),
        "vessels": sch.Schema(
            names=[
                "ais_version",
                "call_sign",
                "class",
                "created_at",
                "flag",
                "general_classification",
                "gross_tonnage",
                "id",
                "imo",
                "individual_classification",
                "last_known_position_accuracy",
                "last_known_position_collection_type",
                "last_known_position_course",
                "last_known_position_draught",
                "last_known_position_geometry",
                "last_known_position_heading",
                "last_known_position_maneuver",
                "last_known_position_rot",
                "last_known_position_speed",
                "last_known_position_timestamp",
                "length",
                "lifeboats",
                "mmsi",
                "most_recent_voyage_destination",
                "most_recent_voyage_eta",
                "name",
                "navigational_status",
                "person_capacity",
                "predicted_position_confidence_radius",
                "predicted_position_course",
                "predicted_position_geometry",
                "predicted_position_speed",
                "predicted_position_timestamp",
                "ship_type",
                "updated_at",
                "width",
            ],
            types=[
                dt.Int64(),
                dt.String(),
                dt.String(),
                dt.Timestamp(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.String(),
                dt.Int64(),
                dt.String(),
                dt.Float64(),
                dt.String(),
                dt.Float64(),
                dt.Float64(),
                dt.Geography(srid=4326),
                dt.Float64(),
                dt.Float64(),
                dt.Float64(),
                dt.Float64(),
                dt.Timestamp(),
                dt.Int64(),
                dt.Float64(),
                dt.Int64(),
                dt.String(),
                dt.Timestamp(),
                dt.String(),
                dt.String(),
                dt.Float64(),
                dt.Float64(),
                dt.Float64(),
                dt.String(),
                dt.Float64(),
                dt.Timestamp(),
                dt.String(),
                dt.Timestamp(),
                dt.Int64(),
            ],
        ),
        "vessels_v1": sch.Schema(
            names=["ais_version", "id", "capacity"],
            types=[
                dt.Int64(),
                dt.String(),
                dt.Struct(
                    names=[
                        "displacement",
                        "dwt",
                        "grain_cubic_capacity",
                        "gross_tonnage",
                        "liquid_cubic_98_percent",
                        "net_tonnage",
                        "teu",
                        "tpcmi",
                        "updated_at",
                    ],
                    types=[
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Float64(),
                        dt.Timestamp(),
                    ],
                ),
            ],
        ),
    }


@pytest.fixture(scope="function")
def client(schemata):
    client = ibis.client.Client()

    def _table(name: str, database: str = None):
        return ops.DatabaseTable(name, schemata[name], client).to_expr()

    client.table = _table

    return client


@pytest.fixture(autouse=True)
def patch_table_equality(monkeypatch):
    """Fixes equality checks to allow for `ops.DatabaseTable == ops.UnboundTable` to be `True`

    This only needs to be True for test cases where we're comparing an AST constructed
    before serialization to it's deserialized form, since we represent tables differently
    after deserialization (using ops.UnboundTable to avoid client binding during deserialization).
    """
    allowed_mismatch_types = (ops.DatabaseTable, ops.UnboundTable)

    def ops_equals(self, other, cache=None):
        if cache is None:
            cache = {}

        key = self, other

        try:
            return cache[key]
        except KeyError:
            if isinstance(self, allowed_mismatch_types) and isinstance(
                other, allowed_mismatch_types
            ):
                cache[key] = result = self is other or (
                    self.name == other.name and self.schema == other.schema
                )
            else:
                cache[key] = result = self is other or (
                    type(self) == type(other)
                    and ops.all_equal(self.args, other.args, cache=cache)
                )
            return result

    def expr_equals(self, other, cache=None):
        types_match = type(self) == type(other) or (
            isinstance(self, allowed_mismatch_types)
            and isinstance(other, allowed_mismatch_types)
        )
        if not types_match:
            return False
        return self._arg.equals(other._arg, cache=cache)

    monkeypatch.setattr(ops.Node, "equals", ops_equals)
    monkeypatch.setattr(ir.Expr, "equals", expr_equals)


def assert_expressions_equal(expr):
    ser = AstSerializer(expr)
    ast = ser.serialize()
    query = make_query(ast, ser.table_refs)
    dsr = AstDeserializer(query)
    res = dsr.deserialize()
    assert ops.all_equal(expr, res)


def test_usecase_1(client):
    expr = client.table("ports_v1")

    assert_expressions_equal(expr)


def test_usecase_2(client):
    table = client.table("ports_v1")
    expr = table[table.port_name, table.port_geom]

    assert_expressions_equal(expr)


def test_usecase_3(client):
    table = client.table("vessels")
    expr = table[table.last_known_position_speed]

    assert_expressions_equal(expr)


def test_usecase_4(client):
    table = client.table("ports_v1")
    expr = table.group_by(table.port_name).aggregate(table.port_name.count())

    assert_expressions_equal(expr)


def test_usecase_6(client):
    point = shapely.geometry.Point(10, -10)
    table = client.table("ports_v1")
    expr = table[table.port_name, table.port_geom][
        table.port_geom.d_within(ibis.literal(point), 5000)
    ]

    assert_expressions_equal(expr)


def test_usecase_7(client):
    point = shapely.geometry.Point(10, -10)
    table = client.table("ports_v1")
    expr = table[table.port_name, table.port_geom][
        table.port_geom.intersects(ibis.literal(point))
    ]

    assert_expressions_equal(expr)


def test_usecase_8(client):
    point = shapely.geometry.Point(10, -10)
    table = client.table("ports_v1")
    expr = table[table.port_name, table.port_geom][
        table.port_geom.equals(ibis.literal(point))
    ]

    assert_expressions_equal(expr)


def test_usecase_9(client):
    vessels = client.table("vessels")
    ports = client.table("ports_v1")

    expr = vessels.inner_join(
        ports, ports.port_geom.d_within(vessels.last_known_position_geometry, 5000)
    )
    expr = expr[ports.port_name, vessels.mmsi]

    assert_expressions_equal(expr)


def test_usecase_10(client):
    vessels = client.table("vessels")
    ports = client.table("ports_v1")
    vessels = vessels[vessels.last_known_position_speed == 0.0]
    expr = vessels.inner_join(
        ports, ports.port_geom.d_within(vessels.last_known_position_geometry, 5000)
    )
    expr = expr[ports.port_name, vessels.mmsi]

    assert_expressions_equal(expr)


def test_usecase_10_variant(client):
    """Variation of use case 10 that doesn't use a selection in the JOIN operation."""
    vessels = client.table("vessels")
    ports = client.table("ports_v1")
    expr = vessels.inner_join(
        ports, ports.port_geom.d_within(vessels.last_known_position_geometry, 5000)
    )
    expr = expr[ports.port_name, vessels.mmsi, vessels.last_known_position_speed]
    expr = expr[expr.last_known_position_speed == 0.0]

    assert_expressions_equal(expr)


def test_use_case_11(client):
    vessels = client.table("vessels")
    ports = client.table("ports_v1")
    vessels = vessels[vessels.last_known_position_speed == 0.0]
    expr = vessels.inner_join(
        ports, ports.port_geom.d_within(vessels.last_known_position_geometry, 5000)
    )
    expr = expr[ports.port_name]
    expr = expr.group_by(expr.port_name).aggregate(expr.port_name.count())

    assert_expressions_equal(expr)


def test_usecase_15(client):
    table = client.table("vessels_v1")
    expr = table[table.capacity["teu"]]

    assert_expressions_equal(expr)


def test_usecase_17(client):
    table = client.table("ports_v1")
    expr = table[table.port_name.name("name")]

    assert_expressions_equal(expr)


def test_usecase_18(client):
    table = client.table("vessels")
    expr = table.aggregate(table.mmsi.count().name("count_vessels"))

    assert_expressions_equal(expr)


def test_usecase_union(client):
    t1 = client.table("vessels")
    t2 = client.table("vessels_v1")

    expr = t1[t1.id].union(t2[t2.id])

    assert_expressions_equal(expr)


def test_usecase_client_union(client):
    t1 = client.table("vessels")
    t2 = t1.view()

    expr = t1.union(t2)

    assert_expressions_equal(expr)


def test_usecase_client_cross_join(client):
    t1 = client.table("vessels")
    t2 = t1.view()

    expr = t1.cross_join(t2)

    assert_expressions_equal(expr)


def test_usecase_cross_join_multi(client):
    t1 = client.table("vessels")
    t2 = client.table("ports_v1")
    t3 = client.table("vessels_v1")
    t4 = t3.view()

    expr = t1.cross_join(t2, t3, t4)

    assert_expressions_equal(expr)
