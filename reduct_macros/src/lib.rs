// Copyright 2023-2024 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proc_macro::TokenStream;
use quote::quote;
use syn::Type::Path;
use syn::{Data, Fields};
#[proc_macro_derive(IntoResponse)]
pub fn into_response_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_into_response(&ast)
}

fn impl_into_response(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let gen = quote! {
        impl axum::response::IntoResponse for #name {
            fn into_response(self) -> axum::response::Response {
                let mut headers = axum_extra::headers::HeaderMap::new();
                headers.typed_insert(axum_extra::headers::ContentType::json());
                (
                    hyper::StatusCode::OK,
                    headers,
                    serde_json::to_string(&self.0).unwrap(),
                )
                    .into_response()
            }
        }
    };
    gen.into()
}

#[proc_macro_derive(Twin)]
pub fn twin_derive(input: TokenStream) -> TokenStream {
    // Construct a representation of Rust code as a syntax tree
    // that we can manipulate
    let ast = syn::parse(input).unwrap();

    // Build the trait implementation
    impl_twin(&ast)
}

fn impl_twin(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let Data::Struct(str) = &ast.data else {
        panic!("Twin derive only works on structs")
    };
    let Fields::Unnamed(field) = str.fields.clone() else {
        panic!("Twin derive only works for unnamed fields")
    };
    let Path(path) = field.unnamed.first().unwrap().ty.clone() else {
        panic!("No type found")
    };

    let derive = &path.path.segments[0].ident;
    let gen = quote! {
        impl From<#derive> for #name {
            fn from(st: #derive) -> Self {
                Self(st)
            }
        }

        impl Into<#derive> for #name {
            fn into(self) -> #derive {
                self.0
            }
        }
    };
    gen.into()
}
