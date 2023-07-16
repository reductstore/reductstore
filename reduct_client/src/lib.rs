fn stub() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        assert!(stub());
    }
}
