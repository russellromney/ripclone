mod common;

use common::*;

#[tokio::test]
async fn add_registers_builds_and_makes_repo_cloneable() {
    setup(false);
    let origin = make_origin("b5_add", "repo");
    origin.commit(&[("a.txt", "1\n")], "c1");
    origin.publish();

    let server = start_server().await;
    let client = server.client();
    let repo_path = format!("{}/{}", origin.owner, origin.repo);

    let added = client.add_repo(&repo_path).await.expect("add repo");
    assert_eq!(added.commit, git(&origin.bare, &["rev-parse", "HEAD"]));

    let added_record = server
        .repo_root
        .join(".ripclone-added")
        .join("github")
        .join("b5_add%2Frepo.json");
    assert!(added_record.exists(), "add must persist added-repo state");

    let (_tmp, clone) = clone_only(
        &server,
        &origin.owner,
        &origin.repo,
        1,
        ripclone::mode::CloneMode::Editable,
    )
    .await
    .expect("clone after add");
    assert_eq!(read(&clone, "a.txt"), "1\n");
}
