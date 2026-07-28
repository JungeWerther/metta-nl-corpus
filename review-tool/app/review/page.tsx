import { getReviewData } from "./actions";
import { ReviewClient } from "./review-client";

export const dynamic = "force-dynamic";

export default async function ReviewPage() {
  const initial = await getReviewData();
  return <ReviewClient initial={initial} />;
}
