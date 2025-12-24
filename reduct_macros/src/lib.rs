// Copyright 2023-2024 ReductSoftware UG
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::parse_quote;
use syn::{
    parse_macro_input, Attribute, Data, Error, Fields, FnArg, Ident, ItemFn, LitStr, Pat, PatIdent,
    ReturnType, Type,
};

/// Wraps the annotated function body in a thread-pool task and exposes a `TaskHandle`.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let description = parse_macro_input!(attr as LitStr);
    let ItemFn {
        attrs,
        vis,
        sig,
        block,
    } = parse_macro_input!(item as ItemFn);

    if sig.asyncness.is_some() {
        return syn::Error::new(
            sig.fn_token.span,
            "#[task] attribute cannot be applied to async functions",
        )
        .to_compile_error()
        .into();
    }

    let attrs = attrs
        .into_iter()
        .filter(|attr| !attr.path().is_ident("task"))
        .collect::<Vec<Attribute>>();

    let output_type = match &sig.output {
        ReturnType::Default => parse_quote!(()),
        ReturnType::Type(_, ty) => *ty.clone(),
    };

    let task_output = ReturnType::Type(
        syn::token::RArrow {
            spans: [Span::call_site(); 2],
        },
        Box::new(parse_quote!(crate::core::thread_pool::TaskHandle<#output_type>)),
    );

    let mut sig_for_wrapper = sig.clone();
    let inner_attrs = attrs.clone();

    let inner_ident = format_ident!("{}_inner", sig.ident);
    let mut inner_sig = sig.clone();
    inner_sig.ident = inner_ident.clone();

    let arg_idents = sig
        .inputs
        .iter()
        .filter_map(|arg| match arg {
            FnArg::Typed(typed) => match &*typed.pat {
                Pat::Ident(PatIdent { ident, .. }) => Some(ident.clone()),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<Ident>>();

    let rest_args = arg_idents.iter().collect::<Vec<_>>();

    let first_arg = sig_for_wrapper.inputs.first_mut();
    if first_arg.is_none() {
        return Error::new(
            sig.fn_token.span,
            "#[task] attribute only supports methods with an Arc<Self> receiver",
        )
        .to_compile_error()
        .into();
    }

    *first_arg.unwrap() = parse_quote!(self: &std::sync::Arc<Self>);
    sig_for_wrapper.output = task_output;

    let inner_call = if rest_args.is_empty() {
        quote!(Self::#inner_ident(this))
    } else {
        quote!(Self::#inner_ident(this, #(#rest_args),*))
    };

    let expanded = quote! {
        #inner_sig #block

        #(#inner_attrs)*
        #vis #sig_for_wrapper {
            let this = std::sync::Arc::clone(self);
            crate::core::thread_pool::spawn(#description, move || #inner_call)
        }
    };

    TokenStream::from(expanded)
}
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
    let Type::Path(path) = field.unnamed.first().unwrap().ty.clone() else {
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
