use super::{split_resource_name, split_tool_name};

fn split_tool_name_owned(tool_name: &str, backend_names: &[&str]) -> Option<(String, String)> {
    split_tool_name(&tool_name, backend_names).map(|pair| (pair.backend_name.to_owned(), pair.tool_name.to_owned()))
}

fn split_resource_name_owned(resource_uri: &str, backend_names: &[&str]) -> Option<(String, String)> {
    split_resource_name(&resource_uri, backend_names)
        .map(|pair| (pair.backend_name.to_owned(), pair.resource_uri.to_owned()))
}

#[test]
fn longest_backend_prefix_wins_for_tools() {
    let backend_names = ["counter", "counter-one"];
    let parsed = split_tool_name_owned("counter-one-increment", &backend_names);
    assert_eq!(parsed, Some(("counter-one".to_owned(), "increment".to_owned())));
}

#[test]
fn longest_backend_prefix_wins_for_resources() {
    let backend_names = ["counter", "counter-one"];
    let parsed = split_resource_name_owned("counter-one-memo://insights", &backend_names);
    assert_eq!(parsed, Some(("counter-one".to_owned(), "memo://insights".to_owned())));
}

#[test]
fn missing_separator_does_not_match() {
    let backend_names = ["counter-one"];
    let parsed = split_tool_name_owned("counter-oneincrement", &backend_names);
    assert_eq!(parsed, None);
}

#[test]
fn hyphen_and_underscore_backend_names_are_distinct() {
    let backend_names = ["counter-one", "counter_one"];
    let hyphenated = split_tool_name_owned("counter-one-increment", &backend_names);
    let underscored = split_tool_name_owned("counter_one-increment", &backend_names);

    assert_eq!(hyphenated, Some(("counter-one".to_owned(), "increment".to_owned())));
    assert_eq!(underscored, Some(("counter_one".to_owned(), "increment".to_owned())));
}
