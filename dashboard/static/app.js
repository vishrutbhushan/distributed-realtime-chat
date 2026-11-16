const get = id => document.getElementById(id);
const escapeHTML = value => String(value ?? "").replace(/[&<>"']/g, char => ({
  "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
})[char]);
const display = value => escapeHTML(value ?? "—");
const clock = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit", minute: "2-digit", second: "2-digit", fractionalSecondDigits: 3, hour12: false,
});
const electionTime = new Intl.DateTimeFormat(undefined, {
  month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit",
});

let current = { nodes: [], events: [] };
let cursor = 0;
let history = [];
let selected = "";
let filter = "all";
let sending = false;
let pickedRequest = "";
let lastDemoRequest = "";
let pendingSelection = "";
let recentlyStopped = "";
let frozenTrace = null;
let activeStep = -1;
let sequence = [];
const byTime = (a, b) => a.time - b.time || a.id - b.id;

const electionKinds = new Set(["pre_vote_started", "pre_vote_reply", "election_started", "vote_cast", "vote_reply", "leader_elected", "follower"]);
const lifecycleKinds = new Set(["control_stop", "control_start", "control_add", "node_online", "node_unreachable", "node_removed"]);

function description(e) {
  switch (e.kind) {
    case "pre_vote_started": return `Probed voters before campaigning for term ${e.term}`;
    case "pre_vote_reply": return `${e.peer} ${e.granted ? "would vote yes" : "declined pre-vote"} for term ${e.term}`;
    case "election_started": return `Became candidate for term ${e.term}, voted for itself (${e.quorum} votes needed)`;
    case "vote_cast": return `${e.granted ? "Voted for" : "Declined"} ${e.peer} in term ${e.term}`;
    case "vote_reply": return `Received ${e.granted ? "yes" : "no"} from ${e.peer} in term ${e.term}`;
    case "leader_elected": return `Won term ${e.term} with ${e.voters.join(", ")}`;
    case "follower": return e.leader ? `Following ${e.leader} in term ${e.term}` : `Became follower in term ${e.term}`;
    case "log_appended": return `Stored ${e.command} at #${e.index}${e.leader ? " from " + e.leader : ""}`;
    case "append_sent": return `Sent #${e.first}–${e.last} to ${e.peer}`;
    case "append_ack": return `${e.peer} confirmed its log matches through #${e.index}`;
    case "commit_advanced": return `Committed through #${e.last}${e.voters ? " with " + e.voters.join(", ") : " after hearing from " + e.leader}`;
    case "entry_applied": return `${e.success ? "Applied" : "Could not apply"} ${e.command} at #${e.index}`;
    case "demo_submitted": return "Submitted a real chat request";
    case "message_forwarded": return `Forwarded the chat write to ${e.peer}`;
    case "demo_confirmed": return "Chat API confirmed the message";
    case "demo_unconfirmed": return "Chat API did not confirm the write";
    case "control_stop": return "Docker stopped the container";
    case "control_start": return "Docker restarted the container";
    case "control_add": return "Docker started a new replica";
    case "node_online": return "Live telemetry available";
    case "node_unreachable": return `Telemetry unavailable (Docker: ${e.docker_state})`;
    case "node_removed": return "Replica removed from Docker";
    case "telemetry_gap": return "The node's event buffer rolled over between samples";
    default: return e.kind.replaceAll("_", " ");
  }
}

let links = [];

function renderMap(nodes, leader) {
  const button = n => {
    const role = n.online ? (n.state === "FOLLOWER" && !n.is_voter ? "Non-voting replica" :
      n.state[0] + n.state.slice(1).toLowerCase()) : n.docker_state === "exited" ? "Stopped" : "Connecting";
    const recovery = history.filter(e => e.node === n.node_id && ["control_add", "control_start"].includes(e.kind))
      .sort(byTime).at(-1);
    const goal = recovery?.target_index;
    const caught = n.online && goal != null && n.last_applied >= goal;
    const progress = !n.online ? (role === "Stopped" ? "Offline" : "Awaiting status")
      : goal != null && !caught ? `Catching up to entry #${goal}`
        : `Last processed entry #${n.last_applied}${caught ? " · caught up" : ""}`;
    return `<button type="button" class="map-node ${n.online && n.state === "LEADER" ? "leader" : ""} ${role === "Stopped" ? "stopped" : ""}"
      aria-pressed="${selected === n.node_id}" data-node="${display(n.node_id)}">
      <strong>${display(n.node_id)}</strong>${display(role)}<small class="${caught ? "caught" : ""}">${display(progress)}</small></button>`;
  };
  const peers = nodes.filter(n => n.node_id !== leader?.node_id);
  const markup = nodes.length ? `<div class="map-leader">${leader ? button(leader) : '<span class="empty">No single leader observed</span>'}</div>
    <div class="map-replicas">${peers.map(button).join("")}</div>` : '<p class="empty">Waiting for replicas.</p>';
  const map = get("map-nodes");
  if (map.dataset.markup !== markup) {
    const focused = map.contains(document.activeElement) ? document.activeElement.dataset.node : null;
    map.innerHTML = markup;
    map.dataset.markup = markup;
    if (focused) [...map.querySelectorAll("[data-node]")].find(el => el.dataset.node === focused)?.focus({preventScroll: true});
  }
  links = leader ? peers.filter(n => n.online && n.leader_id === leader.node_id).map(n => [leader.node_id, n.node_id]) : [];
  drawMapLinks();
}

function drawMapLinks() {
  const canvas = get("map-links");
  const bounds = canvas.getBoundingClientRect();
  if (!bounds.width || !bounds.height) return;
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.round(bounds.width * ratio);
  canvas.height = Math.round(bounds.height * ratio);
  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.strokeStyle = "#8695ad";
  ctx.lineWidth = 1.5;
  const anchors = new Map([...get("map-nodes").querySelectorAll("[data-node]")].map(el => {
    const rect = el.getBoundingClientRect();
    return [el.dataset.node, {x: rect.left + rect.width / 2 - bounds.left, top: rect.top - bounds.top, bottom: rect.bottom - bounds.top}];
  }));
  for (const [leader, follower] of links) {
    const a = anchors.get(leader), b = anchors.get(follower);
    if (!a || !b) continue;
    const rail = a.bottom + 17;
    ctx.beginPath();
    ctx.moveTo(a.x, a.bottom);
    ctx.lineTo(a.x, rail);
    ctx.lineTo(b.x, rail);
    ctx.lineTo(b.x, b.top);
    ctx.stroke();
  }
}

function renderActivity(nodes, leader) {
  const now = Math.floor(Date.now() / 1000);
  const stop = history.filter(e => e.kind === "control_stop").sort(byTime).at(-1);
  const start = get("window").value === "incident" && stop ? Math.floor(stop.time) - 3 : now - 60;
  const end = Math.max(start + 10, now + 1);
  const position = time => Math.max(0, Math.min(100, (time - start) / (end - start) * 100));
  const axis = Array.from({length: 5}, (_, i) => {
    const date = new Date((start + (end - start) * i / 4) * 1000);
    return `<span>${date.toLocaleTimeString([], {hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false})}</span>`;
  }).join("");
  const rows = nodes.map(n => {
    const events = history.filter(e => e.node === n.node_id).sort(byTime);
    const stateNow = n.online ? n.state.toLowerCase() : n.docker_state === "exited" ? "stopped" : "unknown";
    let role = "unknown";
    let reach = "unknown";
    let previous = "unknown";
    let began = start;
    const bars = [];
    const markers = [];
    const transitions = {control_stop: "stop", control_start: "start", control_add: "start", leader_elected: "leader"};
    const pushBar = time => {
      if (position(time) <= position(began)) return;
      bars.push(`<span class="interval state-${previous}" style="left:${position(began)}%;width:${position(time)-position(began)}%" title="${previous}"></span>`);
    };
    for (const e of events) {
      if (e.kind === "leader_elected") { role = "leader"; reach = "online"; }
      if (e.kind === "election_started") { role = "candidate"; reach = "online"; }
      if (e.kind === "follower") { role = "follower"; reach = "online"; }
      if (["control_stop", "node_removed"].includes(e.kind) || (e.kind === "node_unreachable" && e.docker_state === "exited")) {
        reach = "stopped"; role = "unknown";
      } else if (["control_start", "control_add", "node_unreachable"].includes(e.kind)) {
        reach = "unknown"; role = "unknown";
      } else if (e.kind === "node_online") {
        reach = "online";
      }
      const state = reach === "online" ? role : reach;
      if (state !== previous) {
        pushBar(e.time);
        began = Math.max(start, e.time);
        previous = state;
      }
      if (e.time >= start && transitions[e.kind]) {
        markers.push(`<i class="marker ${transitions[e.kind]}" style="left:${position(e.time)}%" title="${display(description(e))}"></i>`);
      }
    }
    pushBar(end);
    const recovery = events.findLast(e => ["control_add", "control_start"].includes(e.kind));
    const goal = recovery?.target_index;
    const caught = n.online && goal != null && n.last_applied >= goal;
    const type = n.is_voter ? "voter" : "replica";
    const progress = n.online && goal != null
      ? (caught ? `Caught up through #${goal}` : `Catching up to #${goal}`)
      : n.online ? `Processed entry #${n.last_applied}` : n.docker_state;
    const behind = caught && leader && n.last_applied < leader.commit_index ? " · behind live" : "";
    return `<div class="activity-row"><button class="node" aria-pressed="${selected === n.node_id}" data-node="${display(n.node_id)}">
      <strong>${display(n.node_id)} · ${stateNow}</strong><small>${type} · <span class="${caught ? "caught" : ""}">${display(progress + behind)}</span></small></button>
      <div class="track" aria-hidden="true">${bars.join("")}${markers.join("")}</div></div>`;
  }).join("");
  const markup = rows ? `<div class="axis"><span>Replica</span><div class="ticks">${axis}</div></div>${rows}`
    : '<div class="empty">No app containers found.</div>';
  const container = get("nodes");
  if (container.dataset.markup !== markup) {
    const focused = document.activeElement?.dataset.node;
    container.innerHTML = markup;
    container.dataset.markup = markup;
    if (focused) [...container.querySelectorAll("[data-node]")].find(el => el.dataset.node === focused)?.focus({preventScroll: true});
  }
}

function messageTrace() {
  const demo = current.demo;
  const messages = new Map();
  for (const event of [...history].sort(byTime)) {
    if (event.kind === "log_appended" && event.request_id && ["SEND_DM", "SEND_GROUP_MSG"].includes(event.command)) {
      messages.set(event.request_id, event);
    }
  }
  if (demo && !messages.has(demo.request_id)) messages.set(demo.request_id, null);
  const options = [...messages.entries()].slice(-12).reverse();
  if (frozenTrace && pickedRequest && !options.some(([id]) => id === pickedRequest)) {
    options.push([pickedRequest, frozenTrace.find(e => e.request_id === pickedRequest && e.kind === "log_appended")]);
  }
  const picker = get("trace-picker");
  const markup = options.map(([id, event]) =>
    `<option value="${display(id)}">${demo?.request_id === id ? display(demo.text) : `Log #${event?.index ?? "—"} · ${display(id.slice(0, 7))}`}</option>`).join("");
  if (picker.dataset.markup !== markup) {
    picker.innerHTML = markup;
    picker.dataset.markup = markup;
  }
  picker.hidden = !options.length;
  if (!frozenTrace && demo?.request_id && demo.request_id !== lastDemoRequest) {
    pickedRequest = demo.request_id;
    lastDemoRequest = demo.request_id;
  }
  if (!options.some(([id]) => id === pickedRequest)) pickedRequest = options[0]?.[0] || "";
  picker.value = pickedRequest;
  if (!pickedRequest) {
    get("trace-label").textContent = "Send a message to follow its log entry.";
    sequence = [];
    renderMessageSteps();
    renderSequence();
    return new Set();
  }
  const source = frozenTrace || history;
  const sameRequest = source.filter(e => e.request_id === pickedRequest).sort(byTime);
  const entries = sameRequest.filter(e => e.kind === "log_appended");
  const entry = entries.find(e => !e.leader) || entries[0];
  const position = entry ? ` · log #${entry.index}, term ${entry.term}` : "";
  get("trace-label").textContent = pickedRequest === demo?.request_id && !frozenTrace
    ? `${demo.text} · via ${demo.node} · ${demo.outcome}${position}`
    : `Observed chat write ${pickedRequest.slice(0, 7)}${position}`;
  const related = new Set(sameRequest.map(e => e.id));
  if (entry) {
    const acknowledged = new Set();
    for (const event of [...source].sort(byTime)) {
      if (event.time < entry.time) continue;
      const originalLeader = event.node === entry.node && event.term === entry.term && !entry.leader;
      const confirmedOnNode = sameRequest.some(e => e.node === event.node &&
        e.index === entry.index && e.term === entry.term &&
        (e.kind === "entry_applied" || e.kind === "log_appended" && e.time <= event.time));
      const ack = originalLeader && event.kind === "append_ack" && event.index >= entry.index && !acknowledged.has(event.peer);
      if (ack) acknowledged.add(event.peer);
      if ((originalLeader && event.kind === "append_sent" && event.first <= entry.index && event.last >= entry.index) || ack ||
        (confirmedOnNode && event.kind === "commit_advanced" && event.first <= entry.index && event.last >= entry.index)) {
        related.add(event.id);
      }
    }
  }
  const sent = new Set();
  sequence = source.filter(e => related.has(e.id)).sort(byTime).filter(e => {
    if (e.kind !== "append_sent") return true;
    const rpc = `${e.node}:${e.peer}:${e.term}:${e.first}:${e.last}`;
    if (sent.has(rpc)) return false;
    sent.add(rpc);
    return true;
  });
  renderMessageSteps();
  renderSequence();
  return related;
}

function renderMessageSteps() {
  const arrival = sequence.find(e => e.kind === "demo_submitted");
  const forward = sequence.find(e => e.kind === "message_forwarded");
  const appended = sequence.filter(e => e.kind === "log_appended");
  const leaderWrite = appended.find(e => !e.leader) || appended[0];
  const copies = [...new Set([
    ...appended.filter(e => e.leader).map(e => e.node),
    ...sequence.filter(e => e.kind === "append_ack").map(e => e.peer),
  ])];
  const quorum = sequence.find(e => e.kind === "commit_advanced" && e.voters);
  const applied = [...new Set(sequence.filter(e => e.kind === "entry_applied" && e.success).map(e => e.node))];
  const accepted = sequence.some(e => e.kind === "demo_confirmed");
  const steps = [
    ["Received", forward ? `${forward.node} → ${forward.peer} (leader)`
      : leaderWrite ? `${leaderWrite.leader || leaderWrite.node} logged entry #${leaderWrite.index}`
        : arrival ? `Request reached ${arrival.node}` : "Awaiting a chat write", !!(arrival || forward || leaderWrite)],
    ["Copied", copies.length ? `Replicated to ${copies.join(", ")}`
      : "Awaiting replication", copies.length > 0],
    ["Committed", quorum ? `Quorum reached by ${quorum.voters.join(", ")}`
      : accepted ? "API confirmed · quorum details unavailable" : "Awaiting a write quorum", !!quorum],
    ["Applied", applied.length ? `Visible on ${applied.join(", ")}` : "Awaiting apply", applied.length > 0],
  ];
  const markup = steps.map(([label, text, observed], index) =>
    `<li class="${observed ? (index >= 2 ? "committed" : "observed") : ""}"><h3><span>${index + 1}.</span>${label}</h3>
      <p>${display(text)}</p></li>`).join("");
  const container = get("message-steps");
  if (container.dataset.markup !== markup) {
    container.innerHTML = markup;
    container.dataset.markup = markup;
  }
}

function sequenceLabel(event) {
  switch (event.kind) {
    case "demo_submitted": return ["Submit"];
    case "message_forwarded": return ["Forward message", "ChatService.SendDirectMessage"];
    case "log_appended": return ["Store entry"];
    case "append_sent": return ["Replicate", "RaftService.AppendEntries"];
    case "append_ack": return ["Acknowledge", "RaftService.AppendEntries reply"];
    case "commit_advanced": return [event.voters ? "Quorum commit" : "Learn commit"];
    case "entry_applied": return [event.success ? "Apply entry" : "Apply failed"];
    case "demo_confirmed": return ["API confirmed"];
    case "demo_unconfirmed": return ["API unconfirmed"];
    default: return [description(event)];
  }
}

function renderSequence() {
  const paused = !!frozenTrace;
  const count = sequence.length;
  activeStep = Math.min(activeStep, count - 1);
  const index = paused ? Math.max(0, activeStep) : count - 1;
  const visible = paused ? sequence.slice(0, index + 1) : sequence;
  get("trace-live").setAttribute("aria-pressed", !paused);
  get("trace-pause").setAttribute("aria-pressed", paused);
  get("trace-pause").disabled = !count;
  get("trace-prev").disabled = !paused || index <= 0;
  get("trace-next").disabled = !paused || index >= count - 1;
  get("trace-position").textContent = count ? (paused ? `Step ${index + 1} / ${count}` : `${count} events`) : "";
  const summary = visible.find(e => e.kind === "commit_advanced" && e.voters);
  get("trace-summary").hidden = !summary;
  get("trace-summary").textContent = summary ? `Commit confirmed by ${summary.voters.join(", ")}.` : "";
  let markup;
  if (!count) {
    markup = `<p class="empty">${pickedRequest ? "Waiting for matching Raft events." : "No chat writes observed yet."}</p>`;
  } else {
    const lanes = [...new Set(sequence.flatMap(e => [e.node, e.peer]).filter(Boolean))]
      .sort((a, b) => a.localeCompare(b, undefined, {numeric: true}));
    const head = lanes.map(n => `<span>${display(n)}</span>`).join("");
    const lines = lanes.map((_, i) => `<i class="lifeline" style="grid-column:${i + 2}" aria-hidden="true"></i>`).join("");
    const t0 = sequence[0].time;
    const steps = visible.map((e, i) => {
      const reverse = e.kind === "append_ack";
      const from = lanes.indexOf(reverse ? e.peer : e.node);
      const to = lanes.indexOf(reverse ? e.node : e.peer);
      const arrow = to >= 0 && to !== from;
      const column = Math.min(from, arrow ? to : from) + 2;
      const span = arrow ? Math.abs(to - from) + 1 : 1;
      const style = `grid-column:${column}/span ${span};--inset:${50 / span}%`;
      const category = e.kind === "commit_advanced" ? "commit" :
        e.kind === "entry_applied" ? (e.success ? "apply" : "failed") : "";
      const focus = paused && i === index;
      const ms = (e.time - t0) * 1000;
      return `<button type="button" class="sequence-grid sequence-step" data-step="${i}" aria-pressed="${focus}"
          aria-label="${display(e.node + ": " + description(e))}">
        <span class="seq-time">+${Math.round(ms)} ms</span>${lines}
        <span class="seq-event ${category} ${arrow ? (to < from ? "arrow back" : "arrow") : ""}" style="${style}">
          <span>${display(sequenceLabel(e)[0])}</span></span></button>`;
    }).join("");
    markup = `<div class="sequence" style="--lanes:${lanes.length}"><div class="sequence-grid sequence-head">
      <span>Elapsed</span>${head}</div>${steps}</div>`;
  }
  const trace = get("trace");
  if (trace.dataset.markup !== markup) {
    const focused = document.activeElement?.dataset.step;
    trace.innerHTML = markup;
    trace.dataset.markup = markup;
    if (focused !== undefined) trace.querySelector(`[data-step="${focused}"]`)?.focus({preventScroll: true});
  }
  const detail = get("trace-detail");
  const event = paused ? visible.at(-1) : null;
  detail.hidden = !event;
  if (event) {
    const rpc = sequenceLabel(event)[1];
    const facts = [rpc, event.term != null && `term ${event.term}`,
      event.index != null ? `log #${event.index}` : event.last != null && `log #${event.first}–${event.last}`].filter(Boolean);
    detail.innerHTML = `<strong>${display(event.node)}</strong> · ${display(description(event))}${facts.length ? `<br><code>${display(facts.join(" · "))}</code>` : ""}`;
  }
}

function election(voters, term, leader) {
  const rounds = new Map();
  const events = [...(current.elections || [])].sort(byTime);
  for (const event of events) {
    if (event.kind !== "election_started" && event.kind !== "leader_elected") continue;
    if (!rounds.has(event.term)) rounds.set(event.term, { candidates: [], winner: null });
    const round = rounds.get(event.term);
    if (event.kind === "election_started") round.candidates.push(event);
    else round.winner = event;
  }
  const terms = [...rounds.keys()].sort((a, b) => b - a).slice(0, 5);
  const latest = rounds.get(terms[0]);
  const candidate = latest?.winner || latest?.candidates.at(-1);
  get("election-label").textContent = latest?.winner
    ? `Latest observed · term ${candidate.term} · ${candidate.node}`
    : candidate ? `Latest observed · term ${candidate.term} · ${candidate.node} campaigning`
      : leader ? `Term ${term} · ${leader.node_id} leads · votes unavailable`
        : "Awaiting election telemetry.";
  const votes = candidate ? voters.map(id => {
    const vote = events.findLast(e => e.term === candidate.term &&
      ((e.kind === "vote_cast" && e.node === id && e.peer === candidate.node) ||
       (e.kind === "vote_reply" && e.node === candidate.node && e.peer === id)));
    const self = id === candidate.node;
    const yes = self || latest.winner?.voters.includes(id) || vote?.granted;
    const state = self ? "self vote" : yes ? "yes" : vote ? "no" : "unobserved";
    return `<li class="vote ${yes ? "yes" : vote ? "no" : ""}"><strong>${display(id)}</strong><span>${state}</span></li>`;
  }).join("") : "";
  const rows = terms.map(number => {
    const { candidates, winner } = rounds.get(number);
    const event = winner || candidates.at(-1);
    const date = new Date(event.time * 1000);
    const outcome = winner ? `${winner.node} won · ${winner.voters.length} votes`
      : `${[...new Set(candidates.map(e => e.node))].join(", ")} · no winner observed`;
    return `<li class="vote term-row"><strong>Term ${display(number)}</strong><span>${display(outcome)}
      <time datetime="${date.toISOString()}">${electionTime.format(date)}</time></span></li>`;
  }).join("") || '<li class="vote">No rounds in recent telemetry.</li>';
  for (const [id, markup] of [["votes", votes], ["recent-terms", rows]]) {
    const list = get(id);
    if (list.dataset.markup !== markup) {
      list.innerHTML = markup;
      list.dataset.markup = markup;
    }
  }
}

function render() {
  const nodes = current.nodes.map(n => n.node_id === recentlyStopped
    ? {...n, online: false, docker_state: "exited"} : n);
  const membership = nodes.find(n => n.online)?.voters || [];
  const withRoles = nodes.map(n => ({ ...n, is_voter: membership.length ? membership.includes(n.node_id) : undefined }));
  const leaders = withRoles.filter(n => n.online && n.state === "LEADER");
  const leader = leaders.length === 1 ? leaders[0] : null;
  const quorum = nodes.find(n => n.online)?.quorum;
  const replicasOnline = nodes.filter(n => n.online).length;
  const votersOnline = withRoles.filter(n => n.online && n.is_voter).length;
  const term = leader?.term ?? (nodes.some(n => n.online) ? Math.max(...nodes.map(n => n.term || 0)) : null);
  const leadership = leader ? `${leader.node_id} leads` : leaders.length > 1 ? "Multiple leaders reported" : "No leader observed";
  const votes = quorum ? `${votersOnline}/${membership.length} voters online` : "Voter state unknown";
  const replicas = nodes.length ? `${replicasOnline}/${nodes.length} replicas online` : "Awaiting replicas";
  get("metrics").innerHTML = `<span><strong>${display(leadership)}</strong></span><span>term ${display(term)}</span>
    <span>${display(replicas)}</span>
    <span class="${quorum && votersOnline < quorum ? "unavailable" : ""}">${display(votes)}</span>
    <span>quorum ${display(quorum)}</span>`;
  get("membership-note").textContent = membership.length
    ? (withRoles.some(n => !n.is_voter)
      ? "Voters elect and commit. Non-voting replicas copy the log and serve chat."
      : `All ${membership.length} configured voters can run for leader.`)
    : "";
  const connection = get("connection");
  connection.textContent = current.error ? "Telemetry unavailable" : `● Live · ${current.project || "cluster"}`;
  connection.title = current.error || "";
  connection.classList.toggle("bad", Boolean(current.error));
  if (pendingSelection && nodes.some(n => n.node_id === pendingSelection)) {
    selected = pendingSelection;
    pendingSelection = "";
  } else if (!nodes.some(n => n.node_id === selected)) {
    selected = nodes[0]?.node_id || "";
  }
  const target = get("target");
  const options = nodes.map(n => `<option value="${display(n.node_id)}">${display(n.node_id)} · ${display(n.docker_state)}</option>`).join("");
  if (target.dataset.options !== options) {
    target.innerHTML = options;
    target.dataset.options = options;
  }
  target.value = selected;
  const busy = sending || current.busy || !!current.error;
  const chosen = nodes.find(n => n.node_id === selected);
  get("send").disabled = busy || !leader;
  get("stop_leader").disabled = busy || !leader;
  get("add").disabled = busy || !nodes.some(n => n.docker_state === "running") || nodes.length >= 12;
  get("stop").disabled = busy || chosen?.docker_state !== "running";
  get("start").disabled = busy || !["exited", "created"].includes(chosen?.docker_state);
  renderMap(withRoles, leader);
  if (get("activity-details").open) renderActivity(withRoles, leader);
  const related = messageTrace();
  election(membership, term, leader);
  const shown = history.filter(e => filter === "all" ||
    (filter === "election" && (electionKinds.has(e.kind) || lifecycleKinds.has(e.kind))) ||
    (filter === "message" && related.has(e.id)))
    .sort((a, b) => b.time - a.time || b.id - a.id).slice(0, 65);
  const timeline = get("timeline");
  const list = shown.map(e => `<li><time>${clock.format(e.time * 1000)}</time><span><b>${display(e.node || "Cluster")}</b> ${display(description(e))}</span></li>`).join("");
  if (timeline.dataset.markup !== list) {
    timeline.innerHTML = list || '<li class="empty">No events in this view yet.</li>';
    timeline.dataset.markup = list;
  }
}

async function poll() {
  try {
    const response = await fetch(`/api/state?after=${cursor}`, { cache: "no-store" });
    if (!response.ok) throw new Error("State request failed");
    current = await response.json();
    if (recentlyStopped && !current.nodes.some(n => n.node_id === recentlyStopped && n.docker_state === "running")) {
      recentlyStopped = "";
    }
    if (current.cursor < cursor) history = [];
    cursor = current.cursor;
    history = [...history, ...current.events].slice(-1800);
  } catch (_) {
    current = { ...current, error: "Connection to the dashboard was interrupted" };
  }
  try { render(); } finally { setTimeout(poll, 350); }
}

async function action(name) {
  const notice = get("notice");
  sending = true;
  notice.textContent = "Running action…";
  notice.classList.remove("bad");
  render();
  try {
    const response = await fetch("/api/action", {
      method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action: name, node: selected || null }),
    });
    const body = await response.json();
    if (!response.ok) throw new Error(body.error || "Action failed");
    if (body.node) pendingSelection = body.node;
    if (name === "stop_leader" || name === "stop") recentlyStopped = body.node || "";
    if (name === "start" && recentlyStopped === body.node) recentlyStopped = "";
    if (name === "send") {
      frozenTrace = null;
      setFilter("message");
    }
    if (name === "stop_leader") {
      get("window").value = "incident";
      setFilter("election");
    }
    if (name === "add" || name === "start") setFilter("all");
    notice.textContent = body.message;
  } catch (error) {
    notice.textContent = error.message;
    notice.classList.add("bad");
  } finally {
    sending = false;
    render();
  }
}

function setFilter(next) {
  filter = next;
  for (const button of get("filters").children) button.setAttribute("aria-pressed", button.dataset.filter === next);
  get("timeline").scrollTop = 0;
}

document.querySelector(".controls").addEventListener("click", e => {
  if (e.target.dataset.action) action(e.target.dataset.action);
});
get("target").addEventListener("change", e => { pendingSelection = ""; selected = e.target.value; render(); });
get("window").addEventListener("change", render);
get("activity-details").addEventListener("toggle", render);
get("trace-picker").addEventListener("change", e => { pickedRequest = e.target.value; frozenTrace = null; render(); });
get("trace-live").addEventListener("click", () => { frozenTrace = null; render(); });
get("trace-pause").addEventListener("click", () => {
  if (frozenTrace) return;
  frozenTrace = [...history];
  activeStep = 0;
  render();
});
get("trace-prev").addEventListener("click", () => { activeStep--; renderSequence(); });
get("trace-next").addEventListener("click", () => { activeStep++; renderSequence(); });
get("trace").addEventListener("click", e => {
  const step = e.target.closest("[data-step]");
  if (!step) return;
  frozenTrace ||= [...history];
  activeStep = Number(step.dataset.step);
  render();
});
function selectNode(e) {
  const button = e.target.closest("[data-node]");
  if (button) { pendingSelection = ""; selected = button.dataset.node; render(); }
}
get("nodes").addEventListener("click", selectNode);
get("map-nodes").addEventListener("click", selectNode);
new ResizeObserver(drawMapLinks).observe(get("map-nodes"));
get("filters").addEventListener("click", e => {
  if (!e.target.dataset.filter) return;
  setFilter(e.target.dataset.filter);
  render();
});
poll();
