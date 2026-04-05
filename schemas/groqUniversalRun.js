"use strict";

function getGroqUniversalRunModel(mongoose) {
  const schema = new mongoose.Schema(
    {
      timestamp: { type: Date, index: true },
      workflow: String,
      symbols_analyzed: [String],
      symbols_count: Number,
      cycles_max: Number,
      target_success_patterns_total: Number,
      min_active_per_side: Number,
      successful_patterns_count: Number,
      failed_patterns_count: Number,
      successful_patterns: [mongoose.Schema.Types.Mixed],
      failed_patterns: [mongoose.Schema.Types.Mixed],
      active_patterns_left: [mongoose.Schema.Types.Mixed],
      attempts: [mongoose.Schema.Types.Mixed],
      token_usage: mongoose.Schema.Types.Mixed,
      final_results: [mongoose.Schema.Types.Mixed],
    },
    { collection: "groq_universal_pattern_runs" }
  );

  return mongoose.models.GroqUniversalRun || mongoose.model("GroqUniversalRun", schema);
}

module.exports = { getGroqUniversalRunModel };

