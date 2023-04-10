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
    if let Ok(env_val) = env::var(env_key) {
        return val;
    }
    false
}

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

// fn get_bool_default(val: bool, env_key: String) -> bool {
//     if val {
//         return val;
//     }
//     if let Ok(mut env_val) = env::var(env_key) {
//         env_val.make_ascii_lowercase();
//         if let Ok(env_bool_val) = env_val.parse::<bool>() {
//             return env_bool_val;
//         }
//     }
//     false
// }
