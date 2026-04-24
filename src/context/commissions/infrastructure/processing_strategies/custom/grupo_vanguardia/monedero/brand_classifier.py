from .brand import Brand


class BrandClassifier:

    ACURA_MODELS = frozenset({
        "ACURA", "RDX", "MDX", "ILX", "TLX", "ILX TECH", "NSX", "RLX", "TLX-L"
    })

    HONDA_MODELS = frozenset({
        "CR-V", "HR-V", "CIVIC", "CITY", "FIT", "BR-V", "PILOT", "ACCORD",
        "ODYSSEY", "PASSPORT", "RIDGELINE", "INSIGHT", "CLARITY"
    })

    def classify(self, model: str) -> Brand:
        if not model:
            return Brand.UNKNOWN

        normalized = str(model).upper().strip()

        if normalized in self.ACURA_MODELS or "ACURA" in normalized:
            return Brand.ACURA

        if normalized in self.HONDA_MODELS:
            return Brand.HONDA

        return Brand.UNKNOWN
