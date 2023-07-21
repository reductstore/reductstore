// Copyright 2023 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proc_macro::TokenStream;
use quote::quote;
use syn;

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
        impl IntoResponse for #name {
            fn into_response(self) -> Response {
                let mut headers = HeaderMap::new();
                headers.typed_insert(headers::ContentType::json());
                (
                    StatusCode::OK,
                    headers,
                    serde_json::to_string(&self.0).unwrap(),
                )
                    .into_response()
            }
        }
    };
    gen.into()
}
