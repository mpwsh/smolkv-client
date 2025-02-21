use reqwest::{Client, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
mod errors;
pub use errors::Error;

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BatchOperation<T> {
    pub key: String,
    pub value: T,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CollectionEvent {
    pub operation: String,
    pub key: String,
    pub value: Value,
    #[serde(default)]
    pub server_time: Option<u64>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct QueryBuilder {
    from: Option<String>,
    to: Option<String>,
    limit: Option<usize>,
    order: Option<SortOrder>,
    #[serde(default)]
    keys: bool,
    query: Option<String>,
}

impl QueryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    pub fn keys(mut self, include_keys: bool) -> Self {
        self.keys = include_keys;
        self
    }

    pub fn order(mut self, order: impl Into<SortOrder>) -> Self {
        self.order = Some(order.into());
        self
    }

    pub fn from(mut self, from: Option<impl Into<String>>) -> Self {
        self.from = from.map(Into::into);
        self
    }

    pub fn to(mut self, to: Option<impl Into<String>>) -> Self {
        self.to = to.map(Into::into);
        self
    }

    pub fn limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }
}

#[derive(Clone)]
pub struct SmolKv {
    endpoint: String,
    client: Client,
}

impl SmolKv {
    pub fn new(endpoint: impl Into<String>, secret: impl Into<String>) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("X-SECRET-KEY", secret.into().parse().unwrap());

        Self {
            endpoint: endpoint.into(),
            client: Client::builder().default_headers(headers).build().unwrap(),
        }
    }

    fn url(&self, path: impl AsRef<str>) -> String {
        let path = path.as_ref().trim_start_matches('/');
        format!("{}/api/{}", self.endpoint, path)
    }

    async fn handle_response<T: DeserializeOwned>(resp: reqwest::Response) -> Result<T> {
        match resp.status() {
            StatusCode::OK | StatusCode::CREATED => Ok(resp.json().await?),
            StatusCode::NOT_FOUND => Err(Error::NotFound(resp.url().path().to_string())),
            StatusCode::CONFLICT => Err(Error::AlreadyExists(resp.url().path().to_string())),
            StatusCode::BAD_REQUEST => Err(Error::BadRequest(resp.text().await?)),
            s => Err(Error::Server(format!("unexpected status: {}", s))),
        }
    }

    // collection operations
    pub async fn collection_exists(&self, name: &str) -> Result<bool> {
        Ok(self
            .client
            .head(self.url(format!("/{}", name)))
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn create_collection(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .put(self.url(format!("/{}", name)))
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn drop_collection(&self, name: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(format!("/{}", name)))
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn list_collection(&self, name: &str, query: QueryBuilder) -> Result<Vec<Value>> {
        let resp = self
            .client
            .get(self.url(format!("/{}", name)))
            .query(&query)
            .send()
            .await?;
        Self::handle_response(resp).await
    }

    pub async fn query_collection(&self, name: &str, query: QueryBuilder) -> Result<Vec<Value>> {
        let resp = self
            .client
            .post(self.url(format!("/{}", name)))
            .json(&query)
            .send()
            .await?;
        Self::handle_response(resp).await
    }
    // key operations
    pub async fn get<T: DeserializeOwned>(&self, collection: &str, key: &str) -> Result<T> {
        let resp = self
            .client
            .get(self.url(format!("/{}/{}", collection, key)))
            .send()
            .await?;

        Self::handle_response(resp).await
    }

    pub async fn put<T: Serialize>(&self, collection: &str, key: &str, value: &T) -> Result<()> {
        let resp = self
            .client
            .put(self.url(format!("/{}/{}", collection, key)))
            .json(value)
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn delete(&self, collection: &str, key: &str) -> Result<()> {
        let resp = self
            .client
            .delete(self.url(format!("/{}/{}", collection, key)))
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn exists(&self, collection: &str, key: &str) -> Result<bool> {
        Ok(self
            .client
            .head(self.url(format!("/{}/{}", collection, key)))
            .send()
            .await?
            .status()
            .is_success())
    }

    pub async fn batch_put<T: Serialize>(
        &self,
        collection: &str,
        items: &[BatchOperation<T>],
    ) -> Result<()> {
        let resp = self
            .client
            .put(self.url(format!("{collection}/_batch")))
            .json(&items)
            .send()
            .await?;

        Self::handle_response::<Value>(resp).await.map(|_| ())
    }

    pub async fn subscribe(&self, collection: &str) -> Result<reqwest::Response> {
        let resp = self
            .client
            .get(self.url(format!("{collection}/_subscribe")))
            .send()
            .await?;

        match resp.status() {
            StatusCode::OK => Ok(resp),
            _ => Err(Error::NotFound(collection.to_string())),
        }
    }
}
