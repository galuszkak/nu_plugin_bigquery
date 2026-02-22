use nu_plugin::{Plugin, PluginCommand};
use tokio::runtime::Runtime;

use crate::commands;

pub struct BigQueryPlugin {
    pub(crate) runtime: Runtime,
}

impl Default for BigQueryPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl BigQueryPlugin {
    pub fn new() -> Self {
        let runtime = Runtime::new().expect("Failed to create tokio runtime");
        Self { runtime }
    }
}

impl Plugin for BigQueryPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![
            // Parent help commands
            Box::new(commands::Bq),
            Box::new(commands::Bigquery),
            // bq * commands
            Box::new(commands::BqQuery),
            Box::new(commands::BqRead),
            Box::new(commands::BqDatasets),
            Box::new(commands::BqTables),
            Box::new(commands::BqSchema),
            // bigquery * aliases
            Box::new(commands::BigqueryQuery),
            Box::new(commands::BigqueryRead),
            Box::new(commands::BigqueryDatasets),
            Box::new(commands::BigqueryTables),
            Box::new(commands::BigquerySchema),
        ]
    }
}
