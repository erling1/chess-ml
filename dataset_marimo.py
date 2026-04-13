import marimo

__generated_with = "0.22.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import chess.pgn
    import sqlite3
    import zstandard
    import io 
    import json

    return json, sqlite3


@app.cell
def _(sqlite3):
    con = sqlite3.connect("chess_dataset.db")
    db = con.cursor()
    db.execute("""
        CREATE TABLE IF NOT EXISTS evaluations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fen TEXT,
            eval REAL
        )
    """)
    return


@app.function
def get_metrics(position):
        
    fen = position["fen"]
    best_eval = max(position["evals"], key=lambda e: e["depth"])  # highest depth
   

    pv = best_eval["pvs"][0]
    if "cp" in pv:
        score = pv["cp"] / 100
    elif "mate" in pv:
        score = 15.0 if pv["mate"] > 0 else -15.0

    return fen, score


@app.cell
def _(json):
    def batch_generator(text_stream, batch_size=10000):
        batch = []
        for line in text_stream:
            position = json.loads(line)

            batch.append(get_metrics(position=position))
        
            if len(batch) == batch_size:
                yield batch
                batch.clear()
            
        

    def commit_batch_to_db(batch, db):
        db.executemany("INSERT INTO evaluations VALUES(?, ?)", batch)
        db.commit()  # Remember to commit the transaction after executing


    return


app._unparsable_cell(
    r"""

    with open("lichess_db_eval.jsonl.zst", "rb") as compressed:
        dctx = zstandard.ZstdDecompressor()
        with dctx.stream_reader(compressed) as reader:
            text_stream = io.TextIOWrapper(reader, encoding="utf-8")

    
            for batch in batch_generator(text_stream)
                commit_batch_to_db(batch=batch, db=db)
        
            

 
    """,
    name="_"
)


@app.cell
def _(testing):
    def display_fen(game):
        board = game.board()
        for node in game.mainline():
            move_san = board.san(node.move)
            board.push(node.move)
            print(move_san, board.fen(), node.comment)

    for i in testing:
        display_fen(i)


    return


if __name__ == "__main__":
    app.run()
