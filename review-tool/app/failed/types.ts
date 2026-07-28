export type FailedItem = {
  id: string;
  annotationId: string;
  premise: string;
  hypothesis: string;
  label: string;
  mettaPremise: string; // original
  mettaHypothesis: string; // original
  premiseFailed: boolean;
  hypothesisFailed: boolean;
  note: string | null;
  isFixed: boolean;
  fixed: {
    fixedMettaPremise: string;
    fixedMettaHypothesis: string;
    note: string | null;
  } | null;
};
