use crate::scylla::{map_fields, mapper_ident};
use proc_macro2::TokenStream;
use quote::quote;
use syn::{DataStruct, Fields, Ident};

pub fn create_struct_implementations(ident: &Ident, data: &DataStruct) -> TokenStream {
    let row_query = row_query(ident, &data.fields);
    let ser_de = ser_de(ident);

    quote! { #row_query #ser_de }
}

pub fn create_enum_implementations(ident: &Ident) -> TokenStream {
    ser_de(ident)
}

fn row_query(ident: &Ident, fields: &Fields) -> TokenStream {
    match &fields {
        Fields::Unit => None,
        Fields::Unnamed(data) => Some(&data.unnamed),
        Fields::Named(data) => Some(&data.named),
    }
    .map(|fields| {
        map_fields(&fields.iter().collect::<Vec<_>>(), None)
            .map(|(_, val, _)| (format!("\"{val}\""), format!(":{val}")))
            .collect::<(Vec<_>, Vec<_>)>()
    })
    .map(|(fields, bindings)| {
        let fields = fields.join(",");
        let bindings = bindings.join(",");
        quote! {
            impl #ident {
                pub const fn scylla_fields() -> &'static str {
                    #fields
                }
                pub const fn scylla_field_bindings() -> &'static str {
                    #bindings
                }
            }
        }
    })
    .unwrap_or_default()
}

fn ser_de(ident: &Ident) -> TokenStream {
    let mapper_ty = mapper_ident(ident);

    quote! {
        impl scylla::serialize::row::SerializeRow for #ident {
            fn serialize(
                &self,
                ctx: &scylla::serialize::row::RowSerializationContext<'_>,
                writer: &mut scylla::serialize::writers::RowWriter<'_>,
            ) -> Result<(), scylla::serialize::SerializationError> {
                let mapper: #mapper_ty = self.clone().into();
                scylla::serialize::row::SerializeRow::serialize(&mapper, ctx, writer)
            }

            fn is_empty(&self) -> bool {
                false
            }
        }

        impl<'frame, 'metadata> scylla::deserialize::row::DeserializeRow<'frame, 'metadata> for #ident {
            fn type_check(specs: &[scylla::frame::response::result::ColumnSpec<'_>]) -> Result<(), scylla::errors::TypeCheckError> {
                #mapper_ty::type_check(specs)
            }

            fn deserialize(
                row: scylla::deserialize::row::ColumnIterator<'frame, 'metadata>,
            ) -> Result<Self, scylla::errors::DeserializationError> {
                <#mapper_ty as scylla::deserialize::row::DeserializeRow<'frame, 'metadata>>::deserialize(row)
                    .and_then(|v| v.try_into().map_err(scylla::errors::DeserializationError::new))
            }
        }
    }
}
