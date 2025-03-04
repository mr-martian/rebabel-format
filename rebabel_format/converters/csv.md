# CSV: Comma-Separated Values

Each row of a CSV file is imported as an `entry`.
The first row is used as the feature names.

```csv
lemma,POS,definition
strontium,N,an element
potato,N,a vegetable
```

* type: `entry`
  * `csv:lemma`: `strontium` (string)
  * `csv:POS`: `N` (string)
  * `csv:definition`: `an element` (string)
* type: `entry`
  * `csv:lemma`: `potato` (string)
  * `csv:POS`: `N` (string)
  * `csv:definition`: `a vegetable` (string)
