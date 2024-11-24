import os
from jinja2 import Environment, FileSystemLoader

semantic_model_file = "support_tickets_semantic_model.yaml"
db_name = "demo_db2"
schema_name = "demo_schema"

curr_path = os.path.abspath(os.path.dirname(__file__))
template_dir = os.path.join(
    curr_path,
    "..",
    "data",
)
template_dir = os.path.abspath(template_dir)

_model_file_template = f"{semantic_model_file}.j2"
env = Environment(
    loader=FileSystemLoader(template_dir),  # Look for templates in 'data' directory
    trim_blocks=True,
    lstrip_blocks=True,
)

_model_file = os.path.join(
    template_dir,
    semantic_model_file,
)

template = env.get_template(_model_file_template)
rendered_yaml = template.render({"db_name": db_name, "schema_name": schema_name})
with open(_model_file, "w") as file:
    file.write(rendered_yaml)
