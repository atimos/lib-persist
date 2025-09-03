mod from;
mod scylla_field;
mod scylla_row;
mod struct_;
mod try_from;

use heck::ToSnekCase;
use quote::{format_ident, quote};
use struct_::Kind;
use syn::{Data, DeriveInput, Error, Field, Ident, Type, parse_macro_input};

pub fn map_to_row(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let result = match input.data {
        Data::Enum(ref data) => Ok((
            struct_::create_enum(&input.ident, data, Kind::Row),
            from::create_enum_implementation(&input.ident, data),
            try_from::create_enum_implementation(&input.ident, data),
            scylla_row::create_enum_implementations(&input.ident),
        )),
        Data::Struct(ref data) => Ok((
            struct_::create_struct(&input.ident, data, Kind::Row),
            from::create_struct_implementation(&input.ident, data),
            try_from::create_struct_implementation(&input.ident, data),
            scylla_row::create_struct_implementations(&input.ident, data),
        )),
        Data::Union(_) => Err(Error::new(
            input.ident.span(),
            "Only enums and structs can derive `MapToScyllaRow`",
        )),
    };

    match result {
        Ok((val1, val2, val3, val4)) => quote! { #val1 #val2 #val3 #val4 },
        Err(error) => error.to_compile_error(),
    }
    .into()
}

pub fn map_to_type(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let result = match input.data {
        Data::Enum(ref data) => Ok((
            struct_::create_enum(&input.ident, data, Kind::Value),
            from::create_enum_implementation(&input.ident, data),
            try_from::create_enum_implementation(&input.ident, data),
            scylla_field::create_enum_implementations(&input.ident, data),
        )),
        Data::Struct(ref data) => Ok((
            struct_::create_struct(&input.ident, data, Kind::Value),
            from::create_struct_implementation(&input.ident, data),
            try_from::create_struct_implementation(&input.ident, data),
            scylla_field::create_struct_implementations(&input.ident, data),
        )),
        Data::Union(_) => Err(Error::new(
            input.ident.span(),
            "Only enums and structs can derive `MapToScyllaType`",
        )),
    };

    match result {
        Ok((val1, val2, val3, val4)) => quote! { #val1 #val2 #val3 #val4 },
        Err(error) => error.to_compile_error(),
    }
    .into()
}

fn map_fields<'a>(
    fields: &'a [&Field],
    variant_name: Option<&'a Ident>,
) -> impl Iterator<Item = (Option<Ident>, Ident, Type)> + 'a {
    let prefix = variant_name.map_or_else(String::new, |name| {
        format!("{}_", name.to_string().to_snek_case())
    });

    fields.iter().enumerate().map(move |(idx, field)| {
        (
            field.ident.clone(),
            format_ident!(
                "{prefix}{}",
                field
                    .ident
                    .as_ref()
                    .map_or_else(|| idx.to_string(), std::string::ToString::to_string)
            ),
            field.ty.clone(),
        )
    })
}

fn mapper_ident(ident: &Ident) -> Ident {
    format_ident!("{ident}ScyllaMapper")
}
