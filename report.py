from IPython.display import display, HTML
import model

STYLES = '''
<style>
  .druid table {
      border: 1px solid black;
      border-collapse: collapse;
  }

  .druid th, .druid td {
      padding: 4px 1em ;
      text-align: left;
  }

  td.druid-right, th.druid-right {
      text-align: right;
  }

  .druid .druid-value {
      text-align: left;
  }
</style>
'''

def html(s):
    s =  '<div class="druid">' + s + '</div>'
    display(HTML(s))

def styles():
    display(HTML(STYLES))

def format_table(heads, rows, styles=None):
    s = '<table>\n'
    if heads is not None:
        s += "<tr>"
        for i in range(len(heads)):
            head = heads[i]
            s += '<th'
            if not styles is None:
                style = styles[i]
                if not style is None and style != "":
                    s += ' class="{}"'.format(styles[i])
            s += '>{}</th>\n'.format(head)
        s += "</tr>\n"
    for row in rows:
        r = "<tr>"
        for i in range(len(row)):
            cell = row[i]
            r += '<td'
            if not styles is None:
                style = styles[i]
                if not style is None and style != "":
                    r += ' class="{}"'.format(styles[i])
            value = "" if cell is None else str(cell)
            r += '>{}</td>'.format(value)
        s += r + "</tr>\n"
    s += "</table>"
    return s

def show_table(heads, rows, styles=None):
    display(HTML(format_table(heads, rows, styles)))

kv_styles = ["druid-right", "druid-value"]
kv_heads = ["Property", "Value"]

def round_dec(n):
    if n < 10_000:
        return n
    if n < 1_000_000:
        return str(int((n + 500) / 1000)) + "K"
    if n < 10_000_000_000:
        return str(int((n + 500_000) / 1_000_000)) + "M"
    return str(int((n + 500_000_000) / 1_000_000_000)) + "B"

BIN_10K = 10 * 1024
BIN_10M = BIN_10K * 1024
BIN_10G = BIN_10M * 1024

def round_bin(n):
    if n < BIN_10K:
        return str(n) + " Bytes"
    if n < BIN_10M:
        return str(int((n + 500) / 1000)) + " Kib"
    if n < BIN_10G:
        return str(int((n + 500_000) / 1_000_000)) + " Mib"
    return str(int((n + 500_000_000) / 1_000_000_000)) + " Gib"

ALL_TOPICS = 'all'
SCHEMA_TOPIC = 'schema'
STORAGE_TOPIC = 'storage'

def table_table(table, topic=ALL_TOPICS):
    row_size = None
    if table.row_count > 0:
        row_size = int(table.size / table.row_count + 0.5)
    rows = [
        ["Name", table.name],
        ["Created", table.create_date],
        ["Records", round_dec(table.row_count)],
        ["Size", round_bin(table.size)]
    ]
    if topic == ALL_TOPICS or topic == SCHEMA_TOPIC:
        details = [
            ["Column Size", round_bin(table.col_size)],
            ["Non-Column Size", round_bin(table.size - table.col_size)],
            ["Row Width", row_size]
        ]
        rows = rows + details
    if topic == ALL_TOPICS or topic == STORAGE_TOPIC:
        details = [
            ["Segments", round_dec(len(table.segments))],
            ["Segment Total", round_bin(table.segment_size)],
            ["Non-Segment Size", round_bin(table.size - table.segment_size)]
        ]
        rows = rows + details
    return format_table(kv_heads, rows, kv_styles)

def detail_table_report(table):
    s = "<h4>Table</h4>\n"
    s += table_table(table)
    for cname in table.column_index:
        col = table.column_index[cname]
        s = "<h4>Column " + col.name + "</h4>\n"
        s += column_details(col)
    html(s)

def column_details(col):
    rows = [
        ["Name", col.name],
        ["Kind", col.kind],
        ["Type", col.type],
        ["Nullable", col.nullable],
        ["Total Size", round_bin(col.total_size)],
        ["Per-Row Size", "{:0.2f}".format(col.per_row_size)],
        ["Of Table Size", "{:.1%}".format(col.size_ratio)]
    ]
    if col.type == model.STRING_TYPE:
        rows.append(["Multi-value", col.is_multi_value])
        rows.append(["Cardinality", round_dec(col.cardinality)])
    return format_table(kv_heads, rows, kv_styles)

def column_table(table):
    rows = []
    for cname in table.column_index:
        col = table.column_index[cname]
        card = None
        mv = None
        if col.type == model.STRING_TYPE:
            mv = col.is_multi_value
            card = round_dec(col.cardinality)
        row = [col.name, col.kind, col.type, col.nullable,
            card, mv,
            round_bin(col.total_size),
            "{:0.2f}".format(col.per_row_size),
            "{:.1%}".format(col.size_ratio)]
        rows.append(row)
    heads = ["Name", "Kind", "Type", "Nulls", "Cardinality", "Multi-value", "Total Size","Row Size", "Row %"]
    styles = [None, None, None, None, "druid-right", None, None, "druid-right", "druid-right"]
    return format_table(heads, rows, styles)

def schema_report(table):
    s = "<h4>Table</h4>\n"
    s += table_table(table, SCHEMA_TOPIC)
    s += "<p><h4>Columns</h4>\n"
    s += column_table(table)
    html(s)

def table_list(druid):
    tables = druid.lead_coordinator().md_ds_names()
    html(format_table(["Table Name"], [[name] for name in tables]))

class TableReport:

    def __init__(self, druid, table_name):
        self.table_name = table_name
        self.model = model.table_model(druid, table_name)

    def schema(self):
       schema_report(self.model)     

class Reports:

    def __init__(self, druid):
        self.druid = druid
    
    def tables(self):
        table_list(self.druid)

    def table(self, table_name):
        return TableReport(self.druid, table_name)