//! The splice generator: file writes planted as Channel transmissions in
//! SWE trajectories, on the synthetic Open-SWE shards
//! (`tests/fixtures/open_swe`).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crosstalk_eval::corpus::{TraceSource, World};
use crosstalk_eval::datasets::chat::ChatMessage;
use crosstalk_eval::datasets::open_swe::{COLUMNS, OpenSweRow, files};
use crosstalk_eval::datasets::parquet_rows::ParquetRows;
use crosstalk_eval::datasets::salt::Selection;
use crosstalk_eval::datasets::swe_splice::read::{OBSERVATION, numbered, view_banner};
use crosstalk_eval::datasets::swe_splice::variant::perturb;
use crosstalk_eval::datasets::swe_splice::write::{heredocs, resolve, writes};
use crosstalk_eval::datasets::swe_splice::{
    DEFAULT_WORKDIR, Pooled, ReadForm, SpliceSource, Variant, WriteForm, insertion_points, plan,
    rewrite_workdir, workdir, world,
};
use crosstalk_eval::pipeline::{ReferenceDetector, run};
use crosstalk_eval::reference::fold::fold;
use crosstalk_eval::truth::{
    CarrierKind, Expectation, ExpectedTransmission, MatchNeed, RouteExpectation, Tier,
};
use crosstalk_spec::derived::flow::resource::Locator;
use crosstalk_spec::derived::provenance::matching::Codec;

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/open_swe")
}

fn pool() -> Vec<Pooled> {
    let mut out = Vec::new();
    for shard in files::discover(&root(), &Selection::default()).unwrap_or_default() {
        let rows = ParquetRows::<OpenSweRow>::open(&root().join(&shard.relative), COLUMNS)
            .unwrap_or_else(|e| panic!("{e}"));
        for row in rows {
            let (row, record) = row.unwrap_or_else(|e| panic!("{e}"));
            out.push(Pooled::new(shard.clone(), row, record));
        }
    }
    out
}

fn by_repo<'a>(pool: &'a [Pooled], repo: &str, harness: &str) -> &'a Pooled {
    pool.iter()
        .find(|p| p.record.repo == repo && p.shard.harness == harness)
        .unwrap_or_else(|| panic!("no {repo} in {harness}"))
}

fn source(splices: usize, seed: u64) -> SpliceSource {
    SpliceSource::open(&root(), &Selection::default(), splices, seed)
        .unwrap_or_else(|e| panic!("{e}"))
}

fn splice_worlds(splices: usize, seed: u64) -> Vec<World> {
    source(splices, seed)
        .worlds()
        .map(|w| w.unwrap_or_else(|e| panic!("{e}")))
        .collect()
}

fn planted(world: &World) -> Vec<&ExpectedTransmission> {
    world
        .truth()
        .iter()
        .filter_map(|e| match e {
            Expectation::Transmission(t) => Some(t),
            _ => None,
        })
        .collect()
}

#[test]
fn heredocs_write_whole_files() {
    let found = heredocs(
        "cat > /tmp/a.py << 'EOF'\nprint(1)\nprint(2)\nEOF\npython /tmp/a.py",
        "/w",
    );
    assert_eq!(
        found,
        vec![("/tmp/a.py".to_owned(), "print(1)\nprint(2)\n".to_owned())]
    );
    let after = heredocs("cat <<EOF > rel/b.py\nx = 1\nEOF", "/w");
    assert_eq!(
        after,
        vec![("/w/rel/b.py".to_owned(), "x = 1\n".to_owned())]
    );
    let cd = heredocs("cd /repo && cat > c.py <<\"END\"\ny = 2\nEND", "/w");
    assert_eq!(cd, vec![("/repo/c.py".to_owned(), "y = 2\n".to_owned())]);
    let tabs = heredocs("cat <<-EOF > d.py\n\tindented\n\tEOF", "/w");
    assert_eq!(tabs, vec![("/w/d.py".to_owned(), "indented\n".to_owned())]);
    let two = heredocs("cat > /a <<A\n1\nA\ncat > /b <<B\n2\nB", "/w");
    assert_eq!(two.len(), 2);
    assert!(heredocs("cat >> /tmp/log <<EOF\nmore\nEOF", "/w").is_empty());
    assert!(heredocs("python3 << EOF\nprint(1)\nEOF", "/w").is_empty());
    assert!(heredocs("cat > /tmp/x <<EOF\nnever closed", "/w").is_empty());
    assert!(heredocs("cat notes.txt && cat > /tmp/y <<EOF\nok\nEOF", "/w").len() == 1);
    assert_eq!(resolve("../x/./y.py", "/a/b"), "/a/x/y.py");
}

#[test]
fn writes_are_found_in_every_harness() {
    let pool = pool();
    let openhands = by_repo(&pool, "acme/widgets", "openhands");
    assert_eq!(openhands.workdir, "/workspace/acme__widgets__1.0");
    let found = openhands.writes();
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].form, WriteForm::EditorCreate);
    assert_eq!(
        found[0].path,
        "/workspace/acme__widgets__1.0/reproduce_color.py"
    );
    assert!(found[0].content.starts_with("\"\"\"Check that a widget"));
    assert_eq!(found[0].message, 6);

    let mini = by_repo(&pool, "omega/stream", "minisweagent");
    assert_eq!(mini.workdir, DEFAULT_WORKDIR);
    let heredoc = mini.writes();
    assert_eq!(heredoc.len(), 1);
    assert_eq!(heredoc[0].form, WriteForm::Heredoc);
    assert_eq!(heredoc[0].path, "/tmp/repro_stream.py");
    assert!(heredoc[0].content.ends_with("dropped'\n"));

    let relative = by_repo(&pool, "omega/sink", "minisweagent");
    assert_eq!(relative.writes()[0].path, "/testbed/check_sink.py");
    // Too short, or no write at all.
    assert!(
        by_repo(&pool, "zeta/registry", "sweagent")
            .writes()
            .is_empty()
    );
}

#[test]
fn read_forms_follow_the_harness() {
    let pool = pool();
    assert_eq!(
        ReadForm::of(&by_repo(&pool, "acme/widgets", "openhands").record.messages),
        ReadForm::EditorView { observation: false }
    );
    assert_eq!(
        ReadForm::of(&by_repo(&pool, "acme/gadgets", "sweagent").record.messages),
        ReadForm::EditorView { observation: true }
    );
    assert_eq!(
        ReadForm::of(
            &by_repo(&pool, "omega/stream", "minisweagent")
                .record
                .messages
        ),
        ReadForm::ShellCat
    );
}

#[test]
fn views_number_lines_like_cat_n() {
    assert_eq!(numbered("a\nb\n"), "     1\ta\n     2\tb\n");
    assert_eq!(numbered("only"), "     1\tonly\n");
    let form = ReadForm::EditorView { observation: true };
    let (message, (start, end)) = form.result("/testbed/x.py", "id-1", "a\nb\n");
    let text = message.text();
    assert!(text.starts_with(OBSERVATION));
    assert_eq!(
        &text[OBSERVATION.len()..start],
        view_banner("/testbed/x.py")
    );
    assert_eq!(&text[start..end], "     1\ta\n     2\tb\n");
    assert_eq!(message.tool_call_id.as_deref(), Some("id-1"));

    let (shell, (start, end)) = ReadForm::ShellCat.result("/testbed/x.py", "id-2", "a\"q\"\n");
    let text = shell.text();
    let parsed: serde_json::Value =
        serde_json::from_str(text).unwrap_or_else(|e| panic!("{e}: {text}"));
    assert_eq!(parsed["returncode"], 0);
    assert_eq!(parsed["output"], "     1\ta\"q\"\n");
    assert_eq!(&text[start..end], "     1\\ta\\\"q\\\"\\n");

    let call = ReadForm::ShellCat.call("/testbed/x.py", "id-2");
    assert_eq!(call.calls()[0].function.name, "bash");
    assert!(call.calls()[0].arguments().contains("cat -n /testbed/x.py"));
    let view = form.call("/testbed/x.py", "id-1");
    assert_eq!(view.calls()[0].function.name, "str_replace_editor");
}

#[test]
fn variants_render_and_need_what_they_should() {
    let content = "def f(a, b):\n    return a + b\n";
    assert_eq!(Variant::Exact.render(content), content);
    let perturbed = Variant::Whitespace.render(content);
    assert_ne!(perturbed, content);
    assert_eq!(perturbed, perturb(content));
    assert_eq!(fold(&perturbed, 0).text, fold(content, 0).text);
    assert!(perturbed.contains("\treturn  a  +  b  \n"));
    assert_eq!(
        Variant::JsonString.render(content),
        "\"def f(a, b):\\n    return a + b\\n\""
    );
    assert_eq!(
        Variant::Base64.render("hi\n"),
        "aGkK",
        "standard base64 of the text"
    );
    let editor = ReadForm::EditorView { observation: false };
    let shell = ReadForm::ShellCat;
    let reach = |need: MatchNeed| (need, Tier::Construction);
    let decoded = |codecs: Vec<Codec>| reach(MatchNeed::Decoded { codecs });
    assert_eq!(Variant::Exact.need(editor), reach(MatchNeed::Exact));
    assert_eq!(
        Variant::Whitespace.need(editor),
        reach(MatchNeed::Normalized)
    );
    assert_eq!(
        Variant::JsonString.need(editor),
        decoded(vec![Codec::JsonString])
    );
    assert_eq!(Variant::Base64.need(editor), decoded(vec![Codec::Base64]));
    // A shell read arrives inside the harness's JSON `output` string.
    assert_eq!(Variant::Exact.need(shell), decoded(vec![Codec::JsonString]));
    assert_eq!(
        Variant::Whitespace.need(shell),
        decoded(vec![Codec::JsonString])
    );
    assert_eq!(
        Variant::Base64.need(shell),
        decoded(vec![Codec::JsonString, Codec::Base64])
    );
    // A JSON string inside a JSON string is two string levels: the spec
    // undoes one (provenance.decode.one-string-level).
    assert_eq!(
        Variant::JsonString.need(shell),
        (
            MatchNeed::Undecodable {
                codec: "json_string+json_string".into()
            },
            Tier::OutOfReach
        )
    );
}

#[test]
fn working_directories_are_made_one() {
    let pool = pool();
    let openhands = by_repo(&pool, "acme/widgets", "openhands");
    let moved = rewrite_workdir(&openhands.record.messages, &openhands.workdir, "/testbed");
    assert_eq!(workdir(&moved), "/testbed");
    let found = writes(&moved, "/testbed");
    assert_eq!(found[0].path, "/testbed/reproduce_color.py");
    assert!(
        moved
            .iter()
            .all(|m| !m.text().contains("/workspace/acme__widgets__1.0"))
    );
    assert_eq!(
        rewrite_workdir(&openhands.record.messages, "/x", "/x"),
        openhands.record.messages
    );
}

#[test]
fn reads_are_spliced_after_a_completed_step() {
    let assistant = |text: &str| ChatMessage {
        role: "assistant".into(),
        content: Some(text.into()),
        ..ChatMessage::default()
    };
    let other = |role: &str| ChatMessage {
        role: role.into(),
        ..ChatMessage::default()
    };
    let messages = vec![
        other("system"),
        other("user"),
        assistant("first"),
        other("tool"),
        assistant("second"),
        assistant("third"),
        other("tool"),
        assistant("fourth"),
    ];
    assert_eq!(insertion_points(&messages), vec![4, 7]);
}

#[test]
fn splices_plant_one_channel_transmission() {
    let worlds = splice_worlds(8, 7);
    assert_eq!(worlds.len(), 8);
    for (number, world) in worlds.iter().enumerate() {
        let variant = Variant::ALL[number % 4];
        assert!(
            world
                .key()
                .as_str()
                .starts_with(&format!("splice-{number:04}-{variant}-")),
            "{}",
            world.key()
        );
        assert_eq!(world.agents().len(), 2);
        let labels = planted(world);
        assert_eq!(labels.len(), 1);
        let label = labels[0].label();
        let nested = variant == Variant::JsonString
            && world.key().as_str().ends_with(ReadForm::ShellCat.name());
        let tier = if nested {
            Tier::OutOfReach
        } else {
            Tier::Construction
        };
        assert_eq!(label.tier, tier);
        assert_eq!(label.carrier, CarrierKind::ToolResult);
        assert!(label.from.name.starts_with("sender/"));
        assert!(label.to.name.starts_with("reader/"));
        let RouteExpectation::Channel {
            resource: Locator::File { host: None, path },
        } = &label.route
        else {
            panic!("a splice is a file channel: {:?}", label.route);
        };
        assert!(path.starts_with('/'));
        assert!(
            label.content.text.contains("     1\\t") || label.content.text.starts_with("     1\t")
        );

        // The sender wrote the file, at the same path, before the reader's
        // exchange that first carries it.
        let reader = world
            .exchange(label.reader_exchange)
            .unwrap_or_else(|| panic!("no reader exchange"));
        let sender = label
            .sender_exchange
            .and_then(|id| world.exchange(id))
            .unwrap_or_else(|| panic!("no sender exchange"));
        assert!(sender.at() < reader.at());
        let file = path.rsplit('/').next().unwrap_or_default();
        let names_file = sender.response().is_some_and(|m| {
            (0..m.part_count()).any(|p| {
                u16::try_from(p)
                    .ok()
                    .and_then(|p| m.part_text(p).ok())
                    .is_some_and(|text| text.contains(file))
            })
        });
        assert!(names_file, "the writing call names {file}");
        assert!(reader.message(label.content.at.part.message).is_some());
        // Only the reader's first exchange after the read carries it as new.
        let earlier: Vec<_> = world
            .exchanges()
            .iter()
            .filter(|e| e.agent() == &label.to && e.at() < reader.at())
            .collect();
        assert!(
            earlier
                .iter()
                .all(|e| e.message(label.content.at.part.message).is_none())
        );
        // The planted pair has no control at the planted exchange.
        assert!(world.truth().iter().all(|e| match e {
            Expectation::NoTransmission(c) => {
                let l = c.label();
                !(l.from == label.from && l.reader_exchange == Some(label.reader_exchange))
            }
            _ => true,
        }));
    }
}

#[test]
fn splices_pair_different_repositories_deterministically() {
    let pool = pool();
    let mut variants = BTreeSet::new();
    for number in 0..12 {
        let first = plan(&pool, number, 3).unwrap_or_else(|e| panic!("{e}"));
        let again = plan(&pool, number, 3).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            (
                first.sender,
                first.reader,
                first.write,
                first.insert_at,
                &first.call_id
            ),
            (
                again.sender,
                again.reader,
                again.write,
                again.insert_at,
                &again.call_id
            )
        );
        assert_ne!(
            pool[first.sender].record.repo,
            pool[first.reader].record.repo
        );
        variants.insert(first.variant);
    }
    assert_eq!(variants.len(), 4);
    let truth = |seed| -> Vec<Vec<Expectation>> {
        splice_worlds(6, seed)
            .iter()
            .map(|w| w.truth().to_vec())
            .collect()
    };
    assert_eq!(truth(11), truth(11));
    assert_ne!(truth(11), truth(12));
}

#[test]
fn a_splice_world_builds_from_a_chosen_pair() {
    let pool = pool();
    let mut chosen = plan(&pool, 0, 0).unwrap_or_else(|e| panic!("{e}"));
    // Force an OpenHands writer into a SWE-agent reader (which has the
    // editor, so it views the file).
    chosen.sender = pool
        .iter()
        .position(|p| p.record.repo == "acme/widgets")
        .unwrap_or_default();
    chosen.write = 0;
    chosen.reader = pool
        .iter()
        .position(|p| p.record.repo == "acme/gadgets" && p.shard.harness == "sweagent")
        .unwrap_or_default();
    chosen.insert_at = 4;
    let built = world(&pool, &chosen).unwrap_or_else(|e| panic!("{e}"));
    let label = planted(&built)[0].label();
    assert_eq!(
        label.route,
        RouteExpectation::Channel {
            resource: Locator::File {
                host: None,
                path: "/testbed/reproduce_color.py".into()
            }
        }
    );
    assert_eq!(label.needs, MatchNeed::Exact);
    assert!(
        label
            .content
            .text
            .starts_with("     1\t\"\"\"Check that a widget")
    );
}

#[test]
fn the_reference_finds_editor_view_splices() {
    let mut splices = source(16, 1);
    let summary = run(
        &mut splices,
        &mut ReferenceDetector::default(),
        1000,
        |_, _| {},
    );
    assert!(summary.failures.is_empty(), "{:?}", summary.failures);
    let missed_editor: Vec<_> = summary
        .score
        .misses
        .iter()
        .filter(|m| {
            m.expectation
                .label()
                .to
                .world
                .as_str()
                .ends_with("editor_view")
        })
        .collect();
    assert!(missed_editor.is_empty(), "{missed_editor:#?}");
    assert_eq!(summary.score.totals.expectations, 16);
}

/// The read call and its result as canonical messages, through the same
/// chat conversion the splice generator uses.
fn spliced_read(
    form: ReadForm,
    path: &str,
    body: &str,
) -> (
    crosstalk_spec::observed::message::ToolCall,
    crosstalk_spec::observed::message::ToolResult,
) {
    use crosstalk_spec::observed::message::{AssistantPart, MessageBody};
    let id = "splice_read";
    let (result, _) = form.result(path, id, body);
    let bodies = crosstalk_eval::datasets::chat::bodies(&[form.call(path, id), result])
        .unwrap_or_else(|e| panic!("{e}"));
    let call = match &bodies[0] {
        MessageBody::Assistant(parts) => parts.iter().find_map(|part| match part {
            AssistantPart::ToolCall(call) => Some(call.clone()),
            _ => None,
        }),
        _ => None,
    }
    .unwrap_or_else(|| panic!("no tool call: {:?}", bodies[0]));
    let result = match &bodies[1] {
        MessageBody::Tool(results) => results.iter().next().cloned(),
        _ => None,
    }
    .unwrap_or_else(|| panic!("no tool result: {:?}", bodies[1]));
    (call, result)
}

/// What crosstalk-flow's real L5 extractors make of a spliced read.
fn extracted(form: ReadForm, path: &str) -> Vec<crosstalk_flow::extract::Classified> {
    use crosstalk_flow::extract::{ConversationContext, ExtractConfig, ToolExtractors};
    let config = ExtractConfig::default();
    let context = ConversationContext::default();
    let extractors = ToolExtractors::new(&config, &context);
    let (call, result) = spliced_read(form, path, "def f():\n    return 42\n");
    extractors
        .extract_classified(&call, Some(&result))
        .unwrap_or_else(|e| panic!("{form}: {e:?}"))
}

const SPLICED_PATH: &str = "/testbed/src/pkg/module.py";

fn file_read(path: &str) -> crosstalk_flow::extract::Classified {
    use crosstalk_flow::extract::ExtractedOp;
    let editor = extracted(ReadForm::EditorView { observation: false }, path);
    assert_eq!(editor.len(), 1, "{editor:?}");
    assert_eq!(editor[0].op, ExtractedOp::Read);
    editor[0].clone()
}

#[test]
fn the_real_extractor_reads_an_editor_view_as_the_file() {
    let read = file_read(SPLICED_PATH);
    assert_eq!(
        read.locator,
        Locator::File {
            host: None,
            path: SPLICED_PATH.to_owned(),
        }
    );
}

#[test]
fn the_real_extractor_reads_a_shell_cat_as_the_same_file() {
    use crosstalk_flow::extract::ExtractedOp;
    let shell = extracted(ReadForm::ShellCat, SPLICED_PATH);
    assert_eq!(shell.len(), 1, "cat -n resolves to one access: {shell:?}");
    assert_eq!(shell[0].op, ExtractedOp::Read);
    assert_eq!(
        shell[0].locator,
        file_read(SPLICED_PATH).locator,
        "the shell read and the editor view name one resource"
    );
}
