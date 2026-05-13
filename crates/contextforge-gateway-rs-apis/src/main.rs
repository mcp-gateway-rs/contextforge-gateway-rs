use std::fs;

use contextforge_gateway_rs_apis::{User, user_store};

use schemars::SchemaGenerator;
use user_store::UserConfig;
#[allow(clippy::print_stdout)]
fn main() -> std::io::Result<()> {
    println!("Generating schema to ./schemas");
    let generator = SchemaGenerator::default();
    let schema = generator.into_root_schema_for::<UserConfig>();
    fs::write("./schemas/user_config.json", serde_json::to_string_pretty(&schema)?)?;
    let generator = SchemaGenerator::default();
    let schema = generator.into_root_schema_for::<User>();
    fs::write("./schemas/user.json", serde_json::to_string_pretty(&schema)?)?;
    Ok(())
}
