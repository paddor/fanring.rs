use std::collections::BTreeMap;
use std::path::Path;

pub(super) fn chart_hardware(fallback: &str) -> String {
    configured_hardware().unwrap_or_else(|| simplify_cpu_name(fallback))
}

fn configured_hardware() -> Option<String> {
    let config = read_chart_hardware();
    if let Some(label) = nonempty_env("FANRING_HW_LABEL").or_else(|| {
        config
            .get("label")
            .cloned()
            .filter(|value| !value.is_empty())
    }) {
        return Some(label);
    }

    let prefix = nonempty_env("FANRING_HW_PREFIX").or_else(|| config.get("prefix").cloned());
    let postfix = nonempty_env("FANRING_HW_POSTFIX").or_else(|| config.get("postfix").cloned());
    join_hardware_parts(prefix, postfix)
}

fn read_chart_hardware() -> BTreeMap<String, String> {
    let working_dir = std::env::current_dir().unwrap_or_default();
    read_chart_hardware_from(&working_dir, Path::new(env!("CARGO_MANIFEST_DIR")))
}

fn read_chart_hardware_from(working_dir: &Path, manifest_dir: &Path) -> BTreeMap<String, String> {
    // A copied or installed binary must use the checkout it is running in.
    let path = working_dir
        .ancestors()
        .map(|dir| dir.join(".chart_hw"))
        .find(|path| path.is_file())
        .unwrap_or_else(|| manifest_dir.join(".chart_hw"));
    let Ok(content) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };

    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| line.split_once('='))
        .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
        .collect()
}

fn nonempty_env(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
}

fn join_hardware_parts(prefix: Option<String>, postfix: Option<String>) -> Option<String> {
    match (prefix, postfix) {
        (Some(prefix), Some(postfix)) => Some(format!("{prefix}, {postfix}")),
        (Some(prefix), None) => Some(prefix),
        (None, Some(postfix)) => Some(postfix),
        (None, None) => None,
    }
}

fn simplify_cpu_name(cpu: &str) -> String {
    cpu.replace("(R)", "")
        .replace("(TM)", "")
        .replace("CPU ", "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::{join_hardware_parts, read_chart_hardware_from, simplify_cpu_name};

    #[test]
    fn reads_runtime_repo_hardware_after_relocated_build() {
        let root =
            std::env::temp_dir().join(format!("fanring-chart-hardware-{}", std::process::id()));
        let repo = root.join("runtime-repo");
        let nested = repo.join("doc/charts");
        let build = root.join("isolated-build");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(&build).unwrap();
        std::fs::write(
            repo.join(".chart_hw"),
            "prefix=runtime host\npostfix=turbo off\n",
        )
        .unwrap();
        std::fs::write(build.join(".chart_hw"), "label=stale build host\n").unwrap();

        let from_root = read_chart_hardware_from(&repo, &build);
        let from_nested = read_chart_hardware_from(&nested, &build);
        std::fs::remove_file(repo.join(".chart_hw")).unwrap();
        let fallback = read_chart_hardware_from(&repo, &build);
        std::fs::remove_file(build.join(".chart_hw")).unwrap();
        let missing = read_chart_hardware_from(&repo, &build);
        std::fs::remove_dir_all(root).unwrap();

        assert_eq!(
            from_root.get("prefix").map(String::as_str),
            Some("runtime host")
        );
        assert_eq!(from_nested, from_root);
        assert_eq!(
            fallback.get("label").map(String::as_str),
            Some("stale build host")
        );
        assert!(missing.is_empty());
    }

    #[test]
    fn joins_configured_hardware_parts() {
        assert_eq!(
            join_hardware_parts(Some("host".to_string()), Some("turbo off".to_string())),
            Some("host, turbo off".to_string())
        );
        assert_eq!(
            join_hardware_parts(Some("host".to_string()), None),
            Some("host".to_string())
        );
    }

    #[test]
    fn simplifies_cpu_brand_markers() {
        assert_eq!(
            simplify_cpu_name("Intel(R) Core(TM) i7 CPU @ 3.20GHz"),
            "Intel Core i7 @ 3.20GHz"
        );
    }
}
