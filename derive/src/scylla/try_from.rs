use crate::{
    is_option, is_vec,
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

    let fields = match &data.fields {
        Fields::Unit => None,
        Fields::Unnamed(data) => Some(generate_try_from_fields(&data.unnamed.iter().collect::<Vec<_>>(), None)),
        Fields::Named(data) => Some(generate_try_from_fields(&data.named.iter().collect::<Vec<_>>(), None)),
    }
    .unwrap_or_default();

    quote! {
        impl TryFrom<#mapper_ty> for #ident {
            type Error = lib_persist::scylla::MappingError;

            fn try_from(val: #mapper_ty) -> Result<Self, Self::Error> {
                Ok(Self {
                    #(#fields),*
                })
            }
        }
    }
}

pub fn create_enum_implementation(ident: &Ident, data: &DataEnum) -> TokenStream {
    if data.variants.iter().all(|v| matches!(v.fields, Fields::Unit)) {
        return quote! {};
    }

    let mapper_ty = mapper_ident(ident);

    let variants = data.variants.iter().map(|v| {
        let variant = v.ident.clone();
        let variant_str = v.ident.to_string().to_snek_case();
        match &v.fields {
            Fields::Unit => quote! { #variant_str => Self::#variant },
            Fields::Unnamed(data) => {
                let values = generate_try_from_fields(&data.unnamed.iter().collect::<Vec<_>>(), Some(&v.ident));

                quote! { #variant_str => Self::#variant(#(#values),*) }
            }
            Fields::Named(data) => {
                let values = generate_try_from_fields(&data.named.iter().collect::<Vec<_>>(), Some(&v.ident));

                quote! { #variant_str => Self::#variant{#(#values),*} }
            }
        }
    });

    quote! {
        impl TryFrom<#mapper_ty> for #ident {
            type Error = lib_persist::scylla::MappingError;

            fn try_from(val: #mapper_ty) -> Result<Self, Self::Error> {
                Ok(match val.kind.as_ref() {
                    #(#variants),*,
                    variant => return Err(lib_persist::scylla::MappingError::InvalidVariant(variant.to_string()))
                })
            }
        }
    }
}

fn generate_try_from_fields(fields: &[&Field], variant_name: Option<&Ident>) -> Vec<TokenStream> {
    map_fields(fields, variant_name)
        .map(|(field, mapper_field, ty)| {
            let mapper_field_str = mapper_field.to_string();
            let field = field.map_or_else(|| quote! {}, |field| quote! {#field: });

            if is_option(&ty) {
                quote! {#field val.#mapper_field}
            } else if is_vec(&ty) {
                quote! {#field val.#mapper_field.unwrap_or_default()}
            } else {
                quote! {#field val.#mapper_field.ok_or_else(|| lib_persist::scylla::MappingError::MissingValue(#mapper_field_str))?}
            }
        })
        .collect()
}
