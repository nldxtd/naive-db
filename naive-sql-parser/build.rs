extern crate lalrpop;

fn main() {
    lalrpop::Configuration::new()
        .emit_rerun_directives(true)
        .force_build(true)
        .emit_whitespace(false)
        .generate_in_source_tree()
        .process()
        .unwrap();
}
