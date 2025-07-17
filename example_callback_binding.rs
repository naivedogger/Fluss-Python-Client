// 回调模式示例
#[pymethods]
impl PyFlussAdmin {
    fn create_table_with_callback(&self, table_path: &PyTablePath, descriptor: &PyTableDescriptor, ignore_if_exists: bool, callback: PyObject) -> PyResult<()> {
        if let (Some(runtime), Some(admin)) = (&self.runtime, &self.admin) {
            let table_path_rust = table_path.inner.clone();
            let descriptor_rust = descriptor.inner.clone();
            let admin_clone = admin.clone(); // 假设可以 clone
            
            // 在后台线程中执行异步操作
            runtime.spawn(async move {
                let result = admin_clone.create_table(&table_path_rust, &descriptor_rust, ignore_if_exists).await;
                
                // 调用 Python 回调
                Python::with_gil(|py| {
                    match result {
                        Ok(_) => {
                            let _ = callback.call1(py, (true, "Success"));
                        }
                        Err(e) => {
                            let _ = callback.call1(py, (false, format!("Error: {}", e)));
                        }
                    }
                });
            });
        }
        Ok(())
    }
}
