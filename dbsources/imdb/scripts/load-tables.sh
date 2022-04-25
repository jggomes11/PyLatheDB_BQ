echo "[1] - Loading IMDB Coffman DB Tables"

bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.casting ./tables/imdb-coffman/casting.tsv ./schemas/imdb-coffman/casting.json
bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.character ./tables/imdb-coffman/character.tsv ./schemas/imdb-coffman/character.json
bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.movie ./tables/imdb-coffman/movie.tsv ./schemas/imdb-coffman/movie.json
bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.movieInfo ./tables/imdb-coffman/movieInfo.tsv ./schemas/imdb-coffman/movieInfo.json
bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.person ./tables/imdb-coffman/person.tsv ./schemas/imdb-coffman/person.json
bq load --autodetect --field_delimiter=tab --null_marker="\N" --quote "" --replace --source_format=CSV --skip_leading_rows=1 imdb.role ./tables/imdb-coffman/role.tsv ./schemas/imdb-coffman/role.json