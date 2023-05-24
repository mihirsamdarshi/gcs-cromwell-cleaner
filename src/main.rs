use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::{arg, Parser};
use futures::future::join_all;
use google_cloud_default::WithAuthExt;
use google_cloud_storage::{
    client::{Client, ClientConfig},
    http::{
        buckets::get::GetBucketRequest,
        objects::{delete::DeleteObjectRequest, list::ListObjectsRequest, Object},
    },
};
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
    static ref RE: Regex = Regex::new(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}/call-[\w_\-]+/shard-\d{1,5}/(?:script|rc|gcs_delocalization\.sh|gcs_localization\.sh|gcs_transfer\.sh|stdout|stderr|pipelines-logs/action/\d+/(?:stderr|stdout))").unwrap();
}

#[derive(Parser, Debug)]
#[command(name = "gcs-cromwell-cleaner")]
#[command(author = "Mihir Samdarshi")]
#[command(version = "0.1")]
#[command(about = "Deletes extraneous Cromwell files from a specified Google Cloud Storage path", long_about = None)]
struct Args {
    #[arg(long, short = 'b', help = "The name of the bucket you want to delete files in", value_name = "gs:// path", value_hint = clap::ValueHint::DirPath)]
    bucket: String,
    #[arg(long, action, help = "Dry run, don't actually delete any files")]
    dry_run: bool,
}

async fn get_client() -> Result<Client> {
    let config = ClientConfig::default().with_auth().await?;
    Ok(Client::new(config))
}

#[derive(Debug)]
struct GsPath {
    bucket: String,
    folder: String,
}

/// Given a string formatted as a gs:// path (e.g.
/// gs://my-bucket/my_folder/my_obj.txt), return the bucket, folder, and object
fn parse_gsutil_path(path: &str) -> Result<GsPath> {
    if !path.starts_with("gs://") {
        return Err(anyhow!("Invalid gsutil path format, no gs:// prefix"));
    }
    // strip off the gs:// prefix
    let path_without_gs = path.strip_prefix("gs://").unwrap();
    // split the path into bucket and folder at the first /
    let parts: Vec<&str> = path_without_gs.splitn(2, '/').collect();
    // if there are less than 2 parts, then there is no folder specified
    if parts.len() < 2 {
        return Err(anyhow!("Invalid gsutil path format, no folder specified"));
    }

    Ok(GsPath {
        bucket: parts[0].to_string(),
        folder: parts[1].to_string(),
    })
}

async fn remove_objects(client: Arc<Client>, items: Vec<Object>) -> Result<()> {
    let mut futures = Vec::with_capacity(items.len());
    for item in items {
        let cloned_client = Arc::clone(&client);
        futures.push(tokio::spawn(async move {
            if let Err(e) = cloned_client
                .delete_object(&DeleteObjectRequest {
                    bucket: item.bucket,
                    object: item.name,
                    ..Default::default()
                })
                .await
            {
                eprintln!("Error deleting object: {}", e);
            };
        }));
    }

    join_all(futures).await;

    Ok(())
}

/// Iterate over each object in the bucket and print the ones that match our
/// regex
async fn filter_objects(items: Vec<Object>) -> Vec<Object> {
    items
        .into_iter()
        // Filter out any objects that don't match our regex
        .filter_map(|obj| {
            if RE.is_match(&obj.name) {
                Some(obj)
            } else {
                None
            }
        })
        .collect()
}

async fn handle_removal(
    items: Option<Vec<Object>>,
    client: Arc<Client>,
    dry_run: bool,
) -> Result<()> {
    if let Some(items) = items {
        let filtered_objects = filter_objects(items).await;
        if dry_run {
            for obj in filtered_objects {
                eprintln!("gs://{}/{}", obj.bucket, obj.name);
            }
        } else {
            remove_objects(client, filtered_objects).await?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Args = Args::parse();
    let client = Arc::new(get_client().await?);

    let gs_path = parse_gsutil_path(&args.bucket)?;

    eprintln!("Listing objects in bucket: {gs_path:?}");

    // verify that the bucket exists
    client
        .get_bucket(&GetBucketRequest {
            bucket: gs_path.bucket.clone(),
            ..Default::default()
        })
        .await?;

    // do our first request
    let mut res = client
        .list_objects(&ListObjectsRequest {
            bucket: gs_path.bucket.to_string(),
            prefix: Some(gs_path.folder.to_string()),
            ..Default::default()
        })
        .await?;

    if args.dry_run {
        println!("Would delete the following objects:");
    }
    // spawn a thread to handle this response
    tokio::spawn(handle_removal(res.items, Arc::clone(&client), args.dry_run));

    while let Some(ref page) = res.next_page_token {
        // do our next request
        res = client
            .list_objects(&ListObjectsRequest {
                bucket: gs_path.bucket.to_string(),
                prefix: Some(gs_path.folder.to_string()),
                page_token: Some(page.to_string()),
                ..Default::default()
            })
            .await?;
        // spawn a thread to handle this list response
        tokio::spawn(handle_removal(
            res.items,
            Arc::clone(&client),
            args.dry_run.clone(),
        ));
    }

    Ok(())
}

// create a module for tests
#[cfg(test)]
mod test {
    use google_cloud_storage::http::objects::Object;

    use super::{filter_objects, parse_gsutil_path};

    #[test]
    fn test_parse_gsutil_path() {
        // test parse_gsutil_path
        let gs_path = parse_gsutil_path("gs://my-bucket/my_folder/").unwrap();
        assert_eq!(gs_path.bucket, "my-bucket");
        assert_eq!(gs_path.folder, "my_folder/");

        // test parse_gsutil_path
        let gs_path = parse_gsutil_path("gs://my-bucket/my_folder/my_obj.txt").unwrap();
        assert_eq!(gs_path.bucket, "my-bucket");
        assert_eq!(gs_path.folder, "my_folder/my_obj.txt");
    }

    #[test]
    #[should_panic]
    fn test_parse_gsutil_path_no_prefix() {
        // test parse_gsutil_path
        let gs_path = parse_gsutil_path("my-bucket/my_folder/my_obj.txt").unwrap();
        assert_eq!(gs_path.bucket, "my-bucket");
        assert_eq!(gs_path.folder, "my_folder/my_obj.txt");
    }

    #[test]
    #[should_panic]
    fn test_parse_gsutil_path_no_folder() {
        // test parse_gsutil_path
        let gs_path = parse_gsutil_path("gs://my-bucket").unwrap();
        assert_eq!(gs_path.bucket, "my-bucket");
        assert_eq!(gs_path.folder, "my_folder/my_obj.txt");
    }

    #[tokio::test]
    async fn test_filter_objects() {
        // test the list_objects function
        let items = vec![
            Object {
                bucket: "my-bucket".to_string(),
                name: "my_folder/b189154b-fd26-4ed1-a6f0-4f6191f1e820/call-foobar/shard-42/script"
                    .to_string(),
                ..Default::default()
            },
            Object {
                bucket: "my-bucket".to_string(),
                name: "my_folder/b189154b-fd26-4ed1-a6f0-4f6191f1e820/call-foobar/shard-42/\
                       my_fake_other_file.bam"
                    .to_string(),
                ..Default::default()
            },
        ];

        let filtered_items = filter_objects(items).await;
        assert_eq!(filtered_items.len(), 1);
        assert_eq!(
            filtered_items[0].name,
            "my_folder/b189154b-fd26-4ed1-a6f0-4f6191f1e820/call-foobar/shard-42/script"
                .to_string()
        );

        let items = vec![
            Object {
                bucket: "my-bucket".to_string(),
                name: "my_folder/call-foobar/shard-42/script".to_string(),
                ..Default::default()
            },
            Object {
                bucket: "my-bucket".to_string(),
                name: "my_folder/call-foobar/shard-42/my_fake_other_file.bam".to_string(),
                ..Default::default()
            },
        ];

        let filtered_items = filter_objects(items).await;
        assert_eq!(filtered_items.len(), 0);
    }
}
