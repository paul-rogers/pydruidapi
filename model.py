import api

STRING_TYPE = "string"
LONG_TYPE = "long"
FLOAT_TYPE = "float"
DOUBLE_TYPE = "double"

TS_KIND = "id"
DIM_KIND = "dim"
METRIC_KIND = "metric"
MIXED_KIND = "mixed"

class ColumnModel:

    def __init__(self, name):
        self.name = name
        self.total_size = 0
        self.per_row_size = 0
        self.size_ratio = 0.0
        self.kind = None
        self.type = None
        self.nullable = False
        self.is_multi_value = False
        self.cardinality = 0

class SegmentModel:

    def __init__(self):
        pass

class TableModel:

    def __init__(self, name):
        self.name = name
        self.row_count = 0
        self.size = 0
        self.col_size = 0
        self.segment_count = 0
        self.columns = []
        self.column_index = {}
        self.intervals = None
        self.rollup = None
        self.query_grain = None
        self.intervals = None
        self.interval_nodes = []
        self.create_date = None
        self.segments = []
        self.segment_size = 0


def table_model(druid, table_name):
    t = TableModel(table_name)
    ds = druid.datasource(table_name)
    analyze_table(ds, t)
    gather_metadata(ds, t)
    return t

def analyze_table(ds, table):
    analysis = ds.analyze()
    table.intervals = analysis['intervals']
    table.size = analysis['size']
    table.row_count = analysis['numRows']
    table.query_grain = analysis['queryGranularity']
    table.rollup = analysis['rollup']
    cols = analysis['columns']
    table.col_size = 0
    for col_name in analysis['columns']:
        tc = ColumnModel(col_name)
        col = cols[col_name]
        tc.total_size = col['size']
        table.col_size += tc.total_size
        tc.per_row_size = tc.total_size / table.row_count
        tc.cardinality = col['cardinality']
        tc.type = col['type'].lower()
        tc.is_multi_value = col['hasMultipleValues']
        tc.nullable = col['hasNulls']
        table.column_index[tc.name] = tc
        if col_name == "__time":
            tc.kind = TS_KIND
    for col_name in table.column_index:
        tc = table.column_index[col_name]
        tc.size_ratio = tc.total_size / table.col_size

def gather_metadata(ds, table):
    table.intervals = ds.intervals()
    md = ds.metadata()
    table.create_date = md['properties']['created']
    for seg in md['segments']:
        table.segments.append(seg)
        table.segment_size += seg['size']
        for col_name in seg['dimensions'].split(','):
            try:
                col = table.column_index[col_name]
            except KeyError:
                col = ColumnModel(col_name)
                col.kind = DIM_KIND
                table.column_index[col_name] = col
                continue
            if col.kind is None:
                col.kind = DIM_KIND
            elif col.kind != DIM_KIND:
                col.kind = MIXED_KIND