from jinja2 import Environment, PackageLoader, select_autoescape

env = Environment(
    loader=PackageLoader("streaming_feature_simulator", "sql_template"),
    autoescape=select_autoescape()
)


SQL_TEMPLATE = {
    "sliding_window": {
        "sql": env.get_template("mega_sql_template.sql")
    },
    "latest": {
        "sql": env.get_template("mega_sql_template.sql")
    }
}


