use devcloud_mail::http::{check_basic_auth, HttpAuth};

#[test]
fn public_basic_auth_helper_handles_all_modes_and_malformed_values() {
    let mut auth = HttpAuth {
        auth_mode: " STRICT ".into(),
        username: "user".into(),
        password: "pass".into(),
    };
    assert!(auth.is_strict());
    assert!(check_basic_auth(&auth, "Basic dXNlcjpwYXNz"));
    for value in [
        "",
        "Basic !!!",
        "Bearer dXNlcjpwYXNz",
        "Basic dXNlcjp3cm9uZw==",
        "éééé",
    ] {
        assert!(!check_basic_auth(&auth, value));
    }
    for mode in ["off", "relaxed", ""] {
        auth.auth_mode = mode.into();
        assert!(!auth.is_strict());
        assert!(check_basic_auth(&auth, ""));
    }
}
