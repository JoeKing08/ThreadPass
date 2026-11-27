export const SCHEMA_VERSION = '1.0.0' as const;

export type TrustTier =
  | 'human_authority'
  | 'unverified_import'
  | 'untrusted_peer'
  | 'untrusted_observation'
  | 'untrusted_unknown';

export type VerificationMethod =
  | 'native_structured_role'
  | 'local_cli_direct'
  | 'unverified_text_import'
  | 'model_reported';

export type ConfirmationPurpose = 'accept_import' | 'authorize_instruction';

export interface ConfirmationProof {
  nonce: string;
  content_sha256: string;
  purpose: ConfirmationPurpose;
  channel: 'local_cli' | 'thp_web';
  confirmed_at: string;
  hmac_hex?: string;
}

export interface ProvenanceChain {
  capture_origin: string;
  verification_method: VerificationMethod;
  source_event_ref: string;
  adapter_version: string;
  confirmation_proof?: ConfirmationProof;
}

export type ContinuityLevel =
  | 'Native Resume'
  | 'Prompt Reconstruction'
  | 'Evidence Recovery';

export type HandoffMode = 'model_authored' | 'evidence_based';

export type WorkspaceKind = 'git_repo' | 'plain_dir' | 'none';

export interface WorkspaceManifest {
  schema_version: string;
  workspace_id: string;
  workspace_kind: WorkspaceKind;
  vcs: 'git' | 'none';
  root_hint?: string;
  remote_url?: string;
  branch?: string;
  head_commit?: string;
  dirty_files: string[];
  untracked_files: string[];
  status_hash: string;
  completeness: 'complete' | 'partial' | 'unobservable';
  blind_spots: string[];
  snapshot_blob_ref?: string;
}

export type SyncPolicy = 'approved' | 'local_only' | 'unscannable_binary';

export interface BlobManifestEntry {
  sha256: string;
  byte_length: number;
  redaction_status: 'clean' | 'redacted' | 'unscannable_binary';
  isolation_level: 'normal' | 'local_only' | 'quarantined';
  sync_policy: SyncPolicy;
  media_type?: string;
}

export type EventPayload =
  | {
      type: 'human_message';
      text: string;
      attachments?: string[];
    }
  | {
      type: 'assistant_part';
      part_type: 'thinking' | 'text' | 'tool_call';
      text?: string;
      opaque_signature?: string;
      call_id?: string;
      tool_name?: string;
      args_json?: string;
      redacted?: boolean;
    }
  | {
      type: 'tool_result';
      call_id: string;
      status: 'succeeded' | 'failed' | 'partial' | 'interrupted';
      is_error: boolean;
      text?: string;
      blob_ref?: string;
    }
  | {
      type: 'coordinator_inject';
      handoff_mode: HandoffMode;
      continuity_level: ContinuityLevel;
      envelope_hash: string;
      summary_text: string;
      conflict_manifest_ref?: string;
    }
  | {
      type: 'cross_stream_message';
      message_id: string;
      from_stream_id: string;
      from_anchor_id?: string;
      target_stream_id: string;
      text?: string;
      blob_ref?: string;
    };

export interface EventRecord {
  schema_version: string;
  kind: 'event';
  event_id: string;
  stream_id: string;
  seq: number;
  commit_txn_id: string;
  trust_tier: TrustTier;
  provenance: ProvenanceChain;
  created_at: string;
  payload: EventPayload;
}

export interface InputProjectionSegment {
  source_event_id: string;
  part_selector?: string;
  projected_role: string;
  modality: string;
  order_index: number;
  transform: 'verbatim' | 'trimmed' | 'summarized' | 'role_mapped' | 'omitted';
  omitted_reason?: string;
}

export interface InvocationRecord {
  schema_version: string;
  kind: 'invocation';
  invocation_id: string;
  stream_id: string;
  commit_txn_id: string;
  invocation_env_ref: string;
  input_refs: string[];
  input_scope: 'exact' | 'inferred' | 'partial' | 'unobservable';
  input_projection: InputProjectionSegment[];
  projection_fidelity:
    | 'exact_observable_payload'
    | 'structured_projection'
    | 'reconstructed'
    | 'unobservable';
  observable_payload_hash?: string;
  output_refs: string[];
  created_at: string;
}

export interface LineageAnchor {
  anchor_id: string;
  thread_id: string;
  stream_id: string;
  parents: string[];
  workspace_manifest: WorkspaceManifest;
  reconciliation_record_ref?: string;
  summary?: string;
  head_event_ids: string[];
  input_dependency_refs?: string[];
  provenance_refs?: string[];
  created_at: string;
}

export interface InputDependencyItem {
  id: string;
  ref_type:
    | 'anchor'
    | 'journal_event'
    | 'artifact'
    | 'cross_stream_message'
    | 'event_range'
    | 'blob';
  target_id: string;
  stream_id?: string;
  thread_id?: string;
  sha256: string;
  range_note?: string;
  observability: 'exact' | 'partial' | 'unobservable';
  required_for_recovery: boolean;
}

export interface InputDependencyManifest {
  input_dependency_id: string;
  items: InputDependencyItem[];
}

export interface ProvenanceRef {
  provenance_ref_id: string;
  claim_or_artifact: string;
  source_type: string;
  source_ref: string;
  note?: string;
}

export interface AdoptedSourceItem {
  source_kind: 'unmanaged_external' | 'foreign_canonical';
  source_project_id: string;
  source_thread_id: string;
  source_stream_id: string;
  source_anchor_id: string;
  ancestor_ids: string[];
  snapshot_blob_ref: string;
  manifest_sha256: string;
  completeness: 'complete' | 'partial' | 'unknown';
  trust_tier: TrustTier;
}

export interface AdoptedSourceManifest {
  adopted_manifest_id: string;
  sources: AdoptedSourceItem[];
}

export interface DispatchRecoveryCapsule {
  capsule_id: string;
  origin_main_stream_id: string;
  origin_thread_id: string;
  source_anchor_id: string;
  dispatch_id: string;
  coordinator_epoch: number;
  coordinator_snapshot_ref: string;
  task_brief: string;
  source_trust_tier: TrustTier;
  initial_context_refs: string[];
  target_workspace_id: string;
  permission_boundary: string[];
  expected_artifacts: string[];
  continuity_level: ContinuityLevel;
  completeness: 'complete' | 'partial';
}

export interface ReconciliationConflict {
  topic: string;
  kind: 'causal' | 'structural' | 'semantic' | 'unknown';
  positions: string[];
  resolution: string;
  human_confirmed: boolean;
}

export interface ReconciliationRecord {
  reconciliation_id: string;
  thread_id: string;
  output_stream_id: string;
  output_anchor_id: string;
  input_sources: Array<{
    stream_id: string;
    anchor_id: string;
    completeness: string;
  }>;
  adopted_items: string[];
  partial_items: string[];
  excluded_items: string[];
  provenance_only_refs: string[];
  conflicts: ReconciliationConflict[];
  next_actions?: string[];
  strategy_id: string;
  strategy_version: string;
  input_manifest_hash: string;
  confirmation_proof?: ConfirmationProof;
  created_at: string;
}

export interface NotifyReceiptRecord {
  receipt_type: 'offered' | 'committed_receipt';
  batch_id: string;
  target_stream_id: string;
  target_base_anchor_id: string;
  message_ids: string[];
  lease_until_mono_ns?: string;
  recorded_at: string;
}

export type LifecycleObservationValue =
  | 'active'
  | 'suspected_unavailable'
  | 'unavailable'
  | 'recovered';

export type LifecycleProjectionValue =
  | LifecycleObservationValue
  | 'concurrent'
  | 'unknown';

export interface LifecycleObservation {
  observation_id: string;
  target_stream_id: string;
  state: LifecycleObservationValue;
  source: string;
  causal_predecessors: string[];
  observed_at: string;
}

export interface RecoveryRecord {
  recovery_id: string;
  recovery_of_stream_id: string;
  recovery_of_anchor_id?: string;
  new_stream_id: string;
  source_anchors: string[];
  gaps: string[];
  created_at: string;
}

export interface BatonHandoffRecord {
  baton_id: string;
  thread_id: string;
  suggested_writer_device_id: string;
  note?: string;
  issued_at: string;
}

export type ControlPayload =
  | { type: 'anchor'; anchor: LineageAnchor }
  | { type: 'blob_manifest'; entries: BlobManifestEntry[] }
  | { type: 'workspace_manifest'; manifest: WorkspaceManifest }
  | { type: 'input_dependency_manifest'; manifest: InputDependencyManifest }
  | { type: 'provenance_ref'; ref: ProvenanceRef }
  | { type: 'adopted_source_manifest'; manifest: AdoptedSourceManifest }
  | { type: 'dispatch_recovery_capsule'; capsule: DispatchRecoveryCapsule }
  | { type: 'reconciliation_record'; record: ReconciliationRecord }
  | { type: 'notify_receipt'; receipt: NotifyReceiptRecord }
  | { type: 'session_lifecycle'; observation: LifecycleObservation }
  | { type: 'recovery_record'; record: RecoveryRecord }
  | { type: 'baton_handoff'; baton: BatonHandoffRecord }
  | { type: 'host_binding'; host_session_id: string; stream_id: string }
  | { type: 'unknown'; original_type: string; raw_json: string; blob_ref?: string };

export interface ControlRecord {
  schema_version: string;
  kind: 'control';
  control_id: string;
  stream_id: string;
  commit_txn_id: string;
  created_at: string;
  payload: ControlPayload;
}

export type StreamShardLine = EventRecord | InvocationRecord | ControlRecord;

export type JournalEventType =
  | 'main_designated'
  | 'dispatch_committed'
  | 'grant_issued'
  | 'grant_revoked'
  | 'result_receipt_recorded'
  | 'result_accepted'
  | 'result_rejected'
  | 'branch_abandoned'
  | 'recovery_recorded'
  | 'adoption_committed'
  | 'reconciliation_committed'
  | 'baton_handoff'
  | 'notify_receipt_ref'
  | 'session_lifecycle_ref';

export interface MainDesignationData {
  main_stream_id: string;
}

export interface DispatchCommittedData {
  dispatch_id: string;
  origin_stream_id: string;
  source_anchor_id: string;
  target_stream_id: string;
  target_workspace_id: string;
  grant_id?: string;
  work_item_id?: string;
  task_brief: string;
  capsule_id: string;
  coordinator_snapshot_ref: string;
  expected_artifacts: string[];
}

export interface GrantIssuedData {
  grant_id: string;
  issuer_stream_id: string;
  grantee_stream_id: string;
  parent_grant_id?: string;
  allowed_actions: string[];
  workspace_scope: string[];
  max_total_spawns: number;
  max_depth: number;
  can_redelegate: boolean;
  expires_at?: string;
}

export interface GrantRevokedData {
  grant_id: string;
  revoked_by_stream_id: string;
  reason: string;
}

export interface ResultReceiptData {
  receipt_id: string;
  dispatch_id?: string;
  target_id: string;
  source_stream_id: string;
  source_anchor_id: string;
  summary: string;
  artifact_refs: string[];
}

export interface ResultDecisionData {
  receipt_id: string;
  target_id: string;
  reason?: string;
}

export interface JournalEventRecord {
  schema_version: string;
  journal_event_id: string;
  thread_id: string;
  main_stream_id: string;
  event_type: JournalEventType;
  actor_stream_id: string;
  causal_predecessors: string[];
  commit_txn_id: string;
  coordinator_epoch: number;
  created_at: string;
  data: Record<string, unknown>;
}

export interface ShardEntry {
  relative_path: string;
  sha256: string;
  stream_id?: string;
  seq?: number;
}

export interface TransactionManifest {
  schema_version: string;
  txn_id: string;
  project_id: string;
  thread_id: string;
  affected_streams: string[];
  expected_parent_heads: Record<string, string | null>;
  expected_journal_head: string | null;
  shards: ShardEntry[];
  journal_shard?: ShardEntry;
  blobs: Array<{
    relative_path: string;
    manifest: BlobManifestEntry;
  }>;
  input_dependency_refs: string[];
  provenance_refs: string[];
  manifest_hash: string;
  created_at: string;
}

export interface CommitIntentProof {
  proof_type: 'signed_attestation';
  mode: 'ed25519_signed' | 'local_hash_integrity';
  txn_id: string;
  project_id: string;
  thread_id: string;
  manifest_hash: string;
  expected_parent_heads: Record<string, string | null>;
  expected_journal_head: string | null;
  fencing_epoch: number;
  signer_device_id: string;
  public_key_spki_b64?: string;
  signature_b64?: string;
  issued_at: string;
}

export interface PublicationClosureProof {
  txn_id: string;
  manifest_hash: string;
  verified_objects_count: number;
  verified_at: string;
  closure_digest: string;
}

export interface TransactionFinalizationCertificate {
  schema_version: string;
  txn_id: string;
  manifest_hash: string;
  commit_intent_proof: CommitIntentProof;
  publication_closure_proof: PublicationClosureProof;
  finalized_at: string;
}

export type LeaseState = 'active' | 'unknown' | 'expired';

export interface LeaseInfo {
  txn_id: string;
  owner_token: string;
  clock_domain_id: string;
  boot_epoch: string;
  lease_until_mono_ns: string;
  fencing_epoch: number;
  pid: number;
  state: LeaseState;
}

export type DeviceKeyStatus = 'active' | 'retired' | 'revoked';

export interface TrustedDeviceKey {
  device_id: string;
  project_id: string;
  thread_id?: string;
  public_key_spki_b64: string;
  status: DeviceKeyStatus;
  pinned_at: string;
  pinned_txn_ids: string[];
  note?: string;
}

export type FsckTxnStatus =
  | 'verified'
  | 'untrusted_unverified_txn'
  | 'incomplete_unverified_txn'
  | 'broken_dependency_closure';

export type StreamProjectionHealth =
  | 'complete'
  | 'partial_blobs_local_only'
  | 'divergent_heads'
  | 'quarantined';
