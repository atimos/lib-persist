use crate::scylla::mapper_ident;
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DataEnum, DataStruct, Fields, Ident, Type};

pub fn create_struct_implementations(ident: &Ident, data: &DataStruct) -> TokenStream {
    let newtype_inner = match &data.fields {
        Fields::Unnamed(data) => match data.unnamed.iter().collect::<Vec<_>>().as_slice() {
            &[field] => Some(&field.ty),
            _ => None,
        },
        _ => None,
    };

    let ser_de = ser_de(ident, newtype_inner, false);

    quote! { #ser_de }
}

pub fn create_enum_implementations(ident: &Ident, data: &DataEnum) -> TokenStream {
    let ser_de = ser_de(
        ident,
        None,
        data.variants
            .iter()
            .all(|v| matches!(v.fields, Fields::Unit)),
    );

    quote! { #ser_de }
}

fn ser_de(ident: &Ident, inner: Option<&Type>, string_enum: bool) -> TokenStream {
    inner.map_or_else(
        || {
            if string_enum {
                ser_de_string(ident)
            } else {
                ser_de_ident_as_type(ident)
            }
        },
        |inner| ser_de_with_type(ident, inner),
    )
}

fn ser_de_string(ident: &Ident) -> TokenStream {
    quote! {
        impl scylla::serialize::value::SerializeValue for #ident {
            fn serialize<'b>(
                &self,
                ty: &scylla::cluster::metadata::ColumnType,
                writer: scylla::serialize::writers::CellWriter<'b>,
            ) -> Result<scylla::serialize::writers::WrittenCellProof<'b>, scylla::errors::SerializationError> {
                scylla::serialize::value::SerializeValue::serialize(&self.to_string(), ty, writer)
            }
        }

        impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata> for #ident {
            fn type_check(ty: &scylla::cluster::metadata::ColumnType) -> Result<(), scylla::errors::TypeCheckError> {
                String::type_check(ty)
            }

            fn deserialize(
                ty: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
                v: Option<scylla::deserialize::FrameSlice<'frame>>,
            ) -> Result<Self, scylla::errors::DeserializationError> {
                <String as scylla::deserialize::value::DeserializeValue<'frame, 'metadata>>::deserialize(ty, v)
                    .and_then(|v| v.parse().map_err(scylla::errors::DeserializationError::new))
            }
        }
    }
}

fn ser_de_ident_as_type(ident: &Ident) -> TokenStream {
    let mapper_ty = mapper_ident(ident);
    quote! {
        impl scylla::serialize::value::SerializeValue for #ident {
            fn serialize<'b>(
                &self,
                ty: &scylla::cluster::metadata::ColumnType,
                writer: scylla::serialize::writers::CellWriter<'b>,
            ) -> Result<scylla::serialize::writers::WrittenCellProof<'b>, scylla::errors::SerializationError> {
                let mapper: #mapper_ty = self.clone().into();
                scylla::serialize::value::SerializeValue::serialize(&mapper, ty, writer)
            }
        }

        impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata> for #ident {
            fn type_check(ty: &scylla::cluster::metadata::ColumnType) -> Result<(), scylla::errors::TypeCheckError> {
                #mapper_ty::type_check(ty)
            }

            fn deserialize(
                ty: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
                v: Option<scylla::deserialize::FrameSlice<'frame>>,
            ) -> Result<Self, scylla::errors::DeserializationError> {
                <#mapper_ty as scylla::deserialize::value::DeserializeValue<'frame, 'metadata>>::deserialize(ty, v)
                    .and_then(|v| v.try_into().map_err(scylla::errors::DeserializationError::new))
            }
        }
    }
}

fn ser_de_with_type(ident: &Ident, inner: &Type) -> TokenStream {
    quote! {
        impl scylla::serialize::value::SerializeValue for #ident {
            fn serialize<'b>(
                &self,
                ty: &scylla::cluster::metadata::ColumnType,
                writer: scylla::serialize::writers::CellWriter<'b>,
            ) -> Result<scylla::serialize::writers::WrittenCellProof<'b>, scylla::errors::SerializationError> {
                scylla::serialize::value::SerializeValue::serialize(&self.0, ty, writer)
            }
        }

        impl<'frame, 'metadata> scylla::deserialize::value::DeserializeValue<'frame, 'metadata> for #ident {
            fn type_check(ty: &scylla::cluster::metadata::ColumnType) -> Result<(), scylla::errors::TypeCheckError> {
                #inner::type_check(ty)
            }

            fn deserialize(
                ty: &'metadata scylla::cluster::metadata::ColumnType<'metadata>,
                v: Option<scylla::deserialize::FrameSlice<'frame>>,
            ) -> Result<Self, scylla::errors::DeserializationError> {
                <#inner as scylla::deserialize::value::DeserializeValue<'frame, 'metadata>>::deserialize(ty, v)
                    .map(Self)
            }
        }
    }
}
