//! Raw exchanges as the proxy hands them to the capture stage: every
//! generation case of testkit's corpus, plus one request carrying an image
//! so the media path is exercised.

use crosstalk_spec::interfaces::l0_ingress::{
    ContentEncoding, DecodedRequest, HarnessRequest, RawExchange, RawResponse,
};
use crosstalk_spec::observed::client::Dialect;
use crosstalk_spec::observed::exchange::{
    Continuation, ExchangeFailure, ExchangeMeta, ModelName, Transport, WireProtocol,
};
use crosstalk_testkit::build::exchange::claude_code_client;
use crosstalk_testkit::corpus::{Endpoint, Expect, anthropic};
use crosstalk_testkit::ids::Ids;
use crosstalk_testkit::time::{T0, millis, secs};

const MODEL: &str = "claude-opus-5-5";

/// A request whose user turn holds a base64 image and a question.
const IMAGE_REQUEST: &str = r#"{"model":"claude-opus-5-5","max_tokens":64,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8gd29ybGQ="}},{"type":"text","text":"what is in this picture?"}]}]}"#;

const IMAGE_RESPONSE: &str = r#"{"id":"msg_01img","type":"message","role":"assistant","model":"claude-opus-5-5","content":[{"type":"text","text":"a greeting"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":20,"output_tokens":3}}"#;

pub fn raw(ids: &mut Ids, body: &[u8], transport: Transport, response: RawResponse) -> RawExchange {
    let client = claude_code_client(ids);
    RawExchange {
        meta: ExchangeMeta {
            id: ids.exchange(),
            protocol: WireProtocol::AnthropicMessages,
            transport,
            model: ModelName(MODEL.to_owned()),
            client,
            started_at: T0,
        },
        request: DecodedRequest {
            harness: HarnessRequest {
                protocol: WireProtocol::AnthropicMessages,
                dialect: Dialect::Reference,
                model: ModelName(MODEL.to_owned()),
                stream: transport == Transport::Sse,
                continuation: Continuation::FullHistory,
            },
            body: body.to_vec(),
            encoding: ContentEncoding::Identity,
        },
        response,
        first_chunk_at: Some(millis(400)),
        ended_at: secs(3),
    }
}

/// Every generation corpus case, then the image request, each with a fresh
/// exchange id from `ids`.
pub fn exchanges(ids: &mut Ids) -> Vec<RawExchange> {
    let cases = anthropic::cases().unwrap_or_else(|error| panic!("the corpus loads: {error}"));
    let mut raws: Vec<RawExchange> = cases
        .iter()
        .filter_map(|case| {
            let Endpoint::Generation { expect, .. } = &case.meta.endpoint else {
                return None;
            };
            let transport = case.response.framing().transport();
            let body = case.response_bytes().to_vec();
            let response = match expect {
                Expect::Failed {
                    failure:
                        failure @ (ExchangeFailure::UpstreamErrorEvent
                        | ExchangeFailure::StreamTruncated
                        | ExchangeFailure::MalformedStream { .. }),
                    ..
                } => RawResponse::Failed {
                    failure: *failure,
                    partial_body: body,
                },
                _ => RawResponse::Complete {
                    status: case.response.status.as_u16(),
                    body,
                },
            };
            Some(raw(ids, case.request.body.as_ref(), transport, response))
        })
        .collect();
    raws.push(raw(
        ids,
        IMAGE_REQUEST.as_bytes(),
        Transport::Http,
        RawResponse::Complete {
            status: 200,
            body: IMAGE_RESPONSE.as_bytes().to_vec(),
        },
    ));
    raws
}
