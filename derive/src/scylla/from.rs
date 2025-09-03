use crate::{
    is_option,
    scylla::{map_fields, mapper_ident},
};
use heck::ToSnekCase;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DataEnum, DataStruct, Field, Fields, Ident};

pub fn create_struct_implementation(ident: &Ident, data: &DataStruct) -> TokenStream {
    if matches!(&data.fields, Fields::Unnamed(data) if data.unnamed.len() == 1) {
        return quote! {};
    }

    let mapper_ty = mapper_ident(ident);

    let (fields, field_names) = match &data.fields {
        Fields::Unit => None,
        Fields::Unnamed(data) => Some(generate_from_fields(
            &data.unnamed.iter().collect::<Vec<_>>(),
            None,
        )),
        Fields::Named(data) => Some(generate_from_fields(
            &data.named.iter().collect::<Vec<_>>(),
            None,
        )),
    }
    .unwrap_or_default();

    quote! {
        impl From<#ident> for #mapper_ty {
            fn from(val: #ident) -> Self {
                let #ident {#(#field_names),*} = val;
                Self {
                    #(#fields),*
                }
            }
        }
    }
}

pub fn create_enum_implementation(ident: &Ident, data: &DataEnum) -> TokenStream {
    if data
        .variants
        .iter()
        .all(|v| matches!(v.fields, Fields::Unit))
    {
        return quote! {};
    }

    let mapper_ty = mapper_ident(ident);

    let variants = data.variants.iter().map(|v| {
        let variant = v.ident.clone();
        let variant_str = v.ident.to_string().to_snek_case();
        match &v.fields {
            Fields::Unit => {
                quote! {
                    #ident::#variant =>  Self {
                        kind: #variant_str.into(),
                        ..Default::default()
                    }
                }
            }
            Fields::Unnamed(data) => {
                let (field_values, fields) =
                    generate_from_fields(&data.unnamed.iter().collect::<Vec<_>>(), Some(&v.ident));
                quote! {
                    #ident::#variant(#(#fields),*) => Self {
                        kind: #variant_str.into(),
                        #(#field_values),*,
                        ..Default::default()
                    }
                }
            }
            Fields::Named(data) => {
                let (field_values, fields) =
                    generate_from_fields(&data.named.iter().collect::<Vec<_>>(), Some(&v.ident));
                quote! {
                    #ident::#variant{#(#fields),*} => Self {
                        kind: #variant_str.into(),
                        #(#field_values),*,
                        ..Default::default()
                    }
                }
            }
        }
    });

    quote! {
        impl From<#ident> for #mapper_ty {
            fn from(val: #ident) -> Self {
                match val {
                    #(#variants),*
                }
            }
        }
    }
}

fn generate_from_fields(
    fields: &[&Field],
    variant_name: Option<&Ident>,
) -> (Vec<TokenStream>, Vec<TokenStream>) {
    map_fields(fields, variant_name)
        .map(|(field, mapper_field, ty)| {
            let field = field.unwrap_or_else(|| mapper_field.clone());
            (
                if is_option(&ty) {
                    quote! {#mapper_field: #field}
                } else {
                    quote! {#mapper_field: Some(#field)}
                },
                quote! {#field},
            )
        })
        .collect()
}
