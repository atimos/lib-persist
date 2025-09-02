use crate::{
    is_option,
    scylla::{map_fields, mapper_ident},
};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DataEnum, DataStruct, Field, Fields, Ident};

pub enum Kind {
    Row,
    Value,
}

pub fn create_struct(ident: &Ident, data: &DataStruct, kind: Kind) -> TokenStream {
    if matches!(&data.fields, Fields::Unnamed(data) if data.unnamed.len() == 1) {
        return quote! {};
    }
    let mapper_ty = mapper_ident(ident);

    let fields = match &data.fields {
        Fields::Unit => None,
        Fields::Unnamed(data) => Some(generate_fields(&data.unnamed.iter().collect::<Vec<_>>(), Some(ident))),
        Fields::Named(data) => Some(generate_fields(&data.named.iter().collect::<Vec<_>>(), None)),
    }
    .unwrap_or_default();

    let derive = match kind {
        Kind::Value => quote! {#[derive(Default, scylla::DeserializeValue, scylla::SerializeValue)]},
        Kind::Row => quote! {#[derive(Default, scylla::DeserializeRow, scylla::SerializeRow)]},
    };

    quote! {
        #derive
        struct #mapper_ty {
            #(#fields),*
        }
    }
}

pub fn create_enum(ident: &Ident, data: &DataEnum, kind: Kind) -> TokenStream {
    let mapper_ty = mapper_ident(ident);

    let fields = data
        .variants
        .iter()
        .filter_map(|v| match &v.fields {
            Fields::Unit => None,
            Fields::Unnamed(data) => Some(generate_fields(&data.unnamed.iter().collect::<Vec<_>>(), Some(&v.ident))),
            Fields::Named(data) => Some(generate_fields(&data.named.iter().collect::<Vec<_>>(), Some(&v.ident))),
        })
        .flatten()
        .collect::<Vec<_>>();

    if fields.is_empty() {
        return quote! {};
    }

    let derive = match kind {
        Kind::Value => quote! {#[derive(Default, scylla::DeserializeValue, scylla::SerializeValue)]},
        Kind::Row => quote! {#[derive(Default, scylla::DeserializeRow, scylla::SerializeRow)]},
    };

    quote! {
        #derive
        struct #mapper_ty {
            kind: String,
            #(#fields),*
        }
    }
}

fn generate_fields(fields: &[&Field], prefix: Option<&Ident>) -> Vec<TokenStream> {
    map_fields(fields, prefix)
        .map(|(_, mapper_field, ty)| {
            let ty = if is_option(&ty) {
                quote! {#ty}
            } else {
                quote! {Option<#ty>}
            };

            quote! { #mapper_field: #ty }
        })
        .collect()
}
