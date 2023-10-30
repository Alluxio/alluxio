/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 * 
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 * 
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

use std::env;
use std::path::Path;

fn generate(spdk_include_path: &Path, output_path: &Path) {
    let bindings = bindgen::builder()
        .clang_arg(format!("-I{}", spdk_include_path.display()))
        .header("wrapper.h")
        .blocklist_item("IPPORT_RESERVED")
        .blocklist_item("FP_NORMAL")
        .blocklist_item("FP_SUBNORMAL")
        .blocklist_item("FP_ZERO")
        .blocklist_item("FP_INFINITE")
        .blocklist_item("FP_NAN")
        .blocklist_type("spdk_nvme_ctrlr_data")
        .generate()
        .expect("Fail to generate bindings!");
    bindings
        .write_to_file(output_path.join("bindings.rs"))
        .expect("Fail to write bindings!");
}

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    println!("cargo:rustc-link-lib=spdk_fat");
    println!("cargo:rustc-link-lib=stdc++");
    println!("cargo:rustc-link-lib=aio");
    println!("cargo:rustc-link-lib=crypto");
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=numa");
    println!("cargo:rustc-link-lib=uuid");
    let spdk_include_path = env::var("SPDK_INCLUDE").unwrap_or("/usr/local/include".to_string());
    let output_path = env::var("OUT_DIR").unwrap();
    generate(Path::new(&spdk_include_path), Path::new(&output_path));
}
