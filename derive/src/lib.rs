#![feature(iter_array_chunks)]

mod scylla;

use syn::Type;

#[proc_macro_derive(MapToScyllaRow)]
pub fn derive_map_to_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    scylla::map_to_row(input)
}

#[proc_macro_derive(MapToScyllaType)]
pub fn derive_map_to_type(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    scylla::map_to_type(input)
}

fn is_option(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => path.path.segments.first().is_some_and(|s| s.ident == "Option"),
        _ => false,
    }
}

fn is_vec(ty: &Type) -> bool {
    match ty {
        Type::Path(path) => path.path.segments.first().is_some_and(|s| s.ident == "Vec"),
        _ => false,
    }
}
