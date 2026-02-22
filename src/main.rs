use nu_plugin::{MsgPackSerializer, serve_plugin};
use nu_plugin_bigquery::BigQueryPlugin;

fn main() {
    serve_plugin(&BigQueryPlugin::new(), MsgPackSerializer {})
}
