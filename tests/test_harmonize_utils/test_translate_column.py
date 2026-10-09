from contextlib import nullcontext
from types import SimpleNamespace

from socio4health.utils import harmonizer_utils


class _FakeScalar:
    def __init__(self, value):
        self.value = value

    def item(self):
        return self.value


class _FakeReduction:
    def __init__(self, values):
        self.values = values

    def max(self):
        return _FakeScalar(max(self.values))


class _FakeTensor:
    def __init__(self, rows):
        self.rows = rows

    def to(self, _device):
        return self

    def sum(self, dim):
        assert dim == 1
        return _FakeReduction([sum(row) for row in self.rows])


class _FakeTokenizer:
    model_max_length = 512
    eos_token_id = 0

    def encode(self, text, add_special_tokens=False):
        assert not add_special_tokens
        return [text] + [None] * (len(text.split()) - 1)

    def decode(self, token_ids, skip_special_tokens=True):
        assert skip_special_tokens
        return token_ids[0]

    def __call__(self, batch, **_kwargs):
        lengths = [len(text.split()) for text in batch]
        return {
            'input_ids': _FakeTensor([[text] for text in batch]),
            'attention_mask': _FakeTensor([[1] * length for length in lengths]),
        }

    def batch_decode(self, generated, skip_special_tokens=True):
        assert skip_special_tokens
        return generated


class _FakeModel:
    config = SimpleNamespace(max_position_embeddings=512)

    def __init__(self):
        self.calls = []

    def generate(self, **kwargs):
        self.calls.append(kwargs)
        translations = {
            'hombre': 'Man, man, man, man, man',
            'cantidad de encuestas cnpv 2018': (
                'number of surveys cnpv 2018 . . . . . . .'
            ),
        }
        return [translations[row[0]] for row in kwargs['input_ids'].rows]


class _FakeTorch:
    @staticmethod
    def inference_mode():
        return nullcontext()


def test_clean_translation_output_removes_degenerate_repetition():
    assert harmonizer_utils._clean_translation_output(
        'number of surveys cnpv 2018 . . . . . . .'
    ) == 'number of surveys cnpv 2018.'
    assert harmonizer_utils._clean_translation_output(
        'Man, man, man, man, man'
    ) == 'Man'


def test_translate_texts_bounds_generation_and_preserves_order(monkeypatch):
    model = _FakeModel()
    runtime = (_FakeTorch(), _FakeTokenizer(), model, 'cpu')
    monkeypatch.setattr(harmonizer_utils, '_get_translation_runtime', lambda: runtime)

    translated = harmonizer_utils._translate_texts([
        'cantidad de encuestas cnpv 2018',
        'hombre',
    ])

    assert translated == ['number of surveys cnpv 2018.', 'Man']
    assert len(model.calls) == 1
    assert model.calls[0]['max_length'] == 32
    assert model.calls[0]['no_repeat_ngram_size'] == 3
    assert model.calls[0]['repetition_penalty'] == 1.1
    assert model.calls[0]['forced_eos_token_id'] == 0
    assert 'max_new_tokens' not in model.calls[0]
