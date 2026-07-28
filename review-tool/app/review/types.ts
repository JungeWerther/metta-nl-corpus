export type ReviewAnnotation = {
  id: string;
  premise: string;
  hypothesis: string;
  label: string;
  mettaPremise: string;
  mettaHypothesis: string;
  sourceIndex: number | null;
  generationModel: string | null;
};

export type ReviewData = {
  position: number;
  total: number;
  annotation: ReviewAnnotation | null;
  verdict: { premiseOk: boolean; hypothesisOk: boolean; note: string | null } | null;
  progress: {
    reviewed: number;
    correct: number;
    failed: number;
    total: number;
    /** Position (in review order) of the first pair with no verdict from this
     * reviewer, or null when every pair has been reviewed. Drives resume-on-
     * reload and the "jump to next unreviewed" control. */
    firstUnreviewed: number | null;
  };
};
