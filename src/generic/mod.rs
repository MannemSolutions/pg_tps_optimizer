use std::env;

pub fn get_env_str(val: &str, env_key: &str, default: &str) -> String {
    if !val.is_empty() {
        return format!("{}", val);
    }
    match env::var(env_key) {
        Ok(env_val) => env_val,
        Err(_e) => format!("{}", default),
    }
}

pub fn get_env_bool(val: bool, env_key: &str) -> bool {
    if val {
        return val;
    }
    if let Ok(_) = env::var(env_key) {
        return true;
    }
    false
}
/*
pub fn get_env_int(val: u32, env_key: &str, default: u32) -> u32 {
    if val > 0 {
        return val;
    }
    if let Ok(env_val) = env::var(env_key) {
        if let Ok(env_int_val) = env_val.parse::<u32>() {
            return env_int_val;
        }
    }
    default
}

fn get_bool_default(val: bool, env_key: String) -> bool {
    if val {
        return val;
    }
    if let Ok(mut env_val) = env::var(env_key) {
        env_val.make_ascii_lowercase();
        if let Ok(env_bool_val) = env_val.parse::<bool>() {
            return env_bool_val;
        }
    }
    false
}
*/
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_env_str() {
        const TEST_ISSET: &str = "is set";
        const TEST_VAR: &str = "TEST_VAR_STR";
        const TEST_VAL: &str = "from env";
        const TEST_DEFAULT: &str = "default";
        env::set_var(TEST_VAR, TEST_VAL);
        assert_eq!(get_env_str("", TEST_VAR, ""), TEST_VAL);
        assert_eq!(get_env_str(TEST_ISSET, TEST_VAR, ""), TEST_ISSET);
        assert_eq!(get_env_str("", TEST_VAR, TEST_DEFAULT), TEST_VAL);
        env::remove_var(TEST_VAR);
        assert_eq!(get_env_str("", TEST_VAR, ""), "");
        assert_eq!(get_env_str(TEST_ISSET, TEST_VAR, ""), TEST_ISSET);
        assert_eq!(get_env_str("", TEST_VAR, TEST_DEFAULT), TEST_DEFAULT);
    }
    #[test]
    fn test_get_env_bool() {
        const TEST_VAR: &str = "TEST_VAR_BOOL";
        const TEST_VAL: &str = "is set";
        env::set_var(TEST_VAR, TEST_VAL);
        for val in [true, false] {
            assert_eq!(get_env_bool(val, TEST_VAR), true);
        }
        env::remove_var(TEST_VAR);
        for val in [true, false] {
            assert_eq!(get_env_bool(val, TEST_VAR), val);
        }
    }
}
