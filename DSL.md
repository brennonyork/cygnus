## DOMAIN SPECIFIC LANGUAGE (DSL)
  Author:  Brennon York
  Email:   brennon.york@gmail.com
  Version: 2.0

## DEFINITION
  A statically typed language created to simplify and define an arbitrary data
  source by specifying it in a record-by-record format. This language was built
  with simplicity for construction, either by human or machine, and as such
  only contains a minimal set of rules. The formalized language follows.

  NOTE: Any term expressed between the '<' and '>' symbols represents a
        variable, not static text. The '-' is used as within regular
	expressions to denote a range of values such as 0-3 to denote 0, 1, 2,
	or 3. Likewise the '?' symbol is used to denote 1 or more occurances
	of the previous value or range.

### `EXPR`
  An Expr (Expression) defines the most basic of components in the DLS. These
  are defined as singular expressions for any one item in a record. The
  chaining of these then creates the full parse definition for the record in
  its entirety. The root form comes in one of two variations. The first form
  (%<Const>) does not contain a Label and is assumed to be garbage (burned 
  off). The second form (%<Const>[<Label>]) contains the Label which allows
  the item to be assigned and evaluated.

```
Expr -> <Expr><Expr>
      | %<Const>
      | %<Const>[<Label>]
```

### `CONST`
  A Const (Constant) is defined as either a set of numbers designating its
  byte size, a Label from a previously defined Expr, or a statically defined
  single character 'S' followed by its string matching set.

```
Const -> 0-9?
       | <Label>
       | S"<StrSet>"
```

### `LABEL`
  A simple designator variable that consists of one or more alphanumeric
  characters with '_' and '-' included. A Label can be evaluated after 
  definition as a Const to define the size of a future Label.

```
Label -> a-zA-Z0-9_-?
```

### `STRINGSET`
  String Sets contain any number of characters to represent the pattern they
  are matching against. Once found the Label will be evaluated as containing
  all characters (bytes) from the position it started up to, but not 
  including, the last character (byte) denoted in the String Set.

## EXAMPLES
  %4
    The most basic of examples. It will skip four bytes from the record and
    will not assign it to any variable.

  %4[Header_Len]
    Like the above, although this will read four bytes and assign them to the
    Label 'Header_Len'.

  %Header_Len[Header]
    This will evaluate the previously defined Label 'Header_Len' into an 
    integer and read that number of bytes assigning those bytes to the Label
    'Header'.

  %S"\""[MyItem]
    The above definition will read from its position until it finds a double
    quote (") character (note that it was escaped) and assigns everything it
    saw up to that point into the Label 'MyItem'.

  %S"]\n"[MySecondItem]
    This example will read all bytes from its position onward until it sees
    the exact match of a bracket (]) followed by a newline (\n). It will then
    assign everything up to the newline, including the bracket, into the Label
    'MySecondItem'.
