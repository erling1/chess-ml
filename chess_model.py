import chess.pgn
import sqlite3
import zstandard
import io
import json
import random
from tqdm import tqdm
from torch.utils.data import IterableDataset
import lightning
import torch
from torch import nn
import numpy as np
from torch.utils.data import DataLoader
from lightning.pytorch.loggers import MLFlowLogger

# con = sqlite3.connect("chess_dataset.db")
# db = con.cursor()
# db.execute("""
#   CREATE TABLE IF NOT EXISTS evaluations (
#       id INTEGER PRIMARY KEY AUTOINCREMENT,
#       bin blob,
#       eval REAL
#   )
# """)


def encode_fen(fen):
    piece_to_idx = {
        "P": 0,
        "N": 1,
        "B": 2,
        "R": 3,
        "Q": 4,
        "K": 5,
        "p": 6,
        "n": 7,
        "b": 8,
        "r": 9,
        "q": 10,
        "k": 11,
    }

    parts = fen.split()
    board_str = parts[0]
    active_color = parts[1]
    castling = parts[2] if len(parts) > 2 else "-"
    en_passant = parts[3] if len(parts) > 3 else "-"

    # 12 pieces x 64 squares = 768
    piece_planes = np.zeros(768, dtype=np.float32)
    square = 0
    for char in board_str:
        if char == "/":
            continue
        elif char.isdigit():
            square += int(char)
        else:
            idx = piece_to_idx[char]
            piece_planes[idx * 64 + square] = 1.0
            square += 1

    # Side to move: 1 bit
    side = np.array([1.0 if active_color == "w" else 0.0], dtype=np.float32)

    # Castling rights: 4 bits (K, Q, k, q)
    castle = np.array(
        [
            1.0 if "K" in castling else 0.0,
            1.0 if "Q" in castling else 0.0,
            1.0 if "k" in castling else 0.0,
            1.0 if "q" in castling else 0.0,
        ],
        dtype=np.float32,
    )

    # En passant: 64 bits (one-hot square, all zeros if none)
    ep = np.zeros(64, dtype=np.float32)
    if en_passant != "-":
        file = ord(en_passant[0]) - ord("a")
        rank = int(en_passant[1]) - 1
        ep[rank * 8 + file] = 1.0

    # Total: 768 + 1 + 4 + 64 = 837
    return np.packbits(
        np.concatenate([piece_planes, side, castle, ep]).astype(np.uint8)
    ).tobytes()


def get_metrics(position):

    fen = position["fen"]
    best_eval = max(position["evals"], key=lambda e: e["depth"])  # highest depth

    pv = best_eval["pvs"][0]
    if "cp" in pv:
        score = pv["cp"] / 100
    elif "mate" in pv:
        score = 15.0 if pv["mate"] > 0 else -15.0

    encoded_fen = encode_fen(fen)

    return encoded_fen, score


def batch_generator(text_stream, batch_size=10000):
    batch = []
    for line in text_stream:
        position = json.loads(line)

        batch.append(get_metrics(position=position))

        if len(batch) == batch_size:
            yield batch
            batch.clear()
    if batch:
        yield batch


def commit_batch_to_db(batch, db, con):
    db.executemany("INSERT INTO evaluations (bin, eval) VALUES(?, ?)", batch)
    con.commit()


# with open("lichess_db_eval.jsonl.zst", "rb") as compressed:
#    dctx = zstandard.ZstdDecompressor()
#    with dctx.stream_reader(compressed) as reader:
#        text_stream = io.TextIOWrapper(reader, encoding="utf-8")
#
#
#        total_batches = 369_477_725 // 10_000 + 1  # 36,948
#
#        for batch in tqdm(batch_generator(text_stream), total=total_batches):
#
#            commit_batch_to_db(batch=batch, db=db, con=con)


def display_fen(game):
    board = game.board()
    for node in game.mainline():
        move_san = board.san(node.move)
        board.push(node.move)
        print(move_san, board.fen(), node.comment)


class ChessDataset(IterableDataset):
    def __init__(self, db_path, table_name):
        self.db_path = db_path
        self.table_name = table_name
        # Connect once to get the total row count
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        self.count = cursor.fetchone()[0]
        conn.close()

    def __len__(self):
        return self.count

    def __iter__(self):
        worker_info = torch.utils.data.get_worker_info()
        conn = sqlite3.connect(self.db_path)

        # modulus perfectly partitions the stream
        query = f"""
                SELECT bin, eval FROM {self.table_name}
                WHERE id % {worker_info.num_workers} = {worker_info.id}"""

        cursor = conn.execute(query)
        buffer_size = 10000
        buffer = []
        for row in cursor:
            if len(buffer) < buffer_size:
                buffer.append(row)
                continue
            idx = random.randint(0, buffer_size - 1)
            out = buffer[idx]
            buffer[idx] = row
            bits = np.unpackbits(np.frombuffer(out[0], dtype=np.uint8)).astype(
                np.float32
            )[:837]
            x = torch.tensor(bits)
            y = torch.tensor(out[1], dtype=torch.float32)
            yield x, y

        random.shuffle(buffer)
        for out in buffer:
            bits = np.unpackbits(np.frombuffer(out[0], dtype=np.uint8)).astype(
                np.float32
            )[:837]
            x = torch.tensor(bits)
            y = torch.tensor(out[1], dtype=torch.float32)
            yield x, y

        conn.close()


class ChessModel(lightning.LightningModule):
    def __init__(
        self, lr=1e-3, batch_size=1024, layer_count=6, in_feat=837, out_feat=1
    ):
        super().__init__()
        self.lr = lr
        self.batch_size = batch_size
        layers = []
        for i in range(layer_count):
            layers.append(nn.Linear(in_feat, in_feat))
            layers.append(nn.ReLU())
        layers.append(nn.Linear(in_feat, out_feat))
        self.seg = nn.Sequential(*layers)

    def training_step(self, batch, batch_idx):
        x, y = batch
        y_hat = self.seg(x)
        loss = nn.functional.l1_loss(y_hat.squeeze(), y)
        self.log("train_loss", loss, on_step=True, on_epoch=True)

        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=self.lr)


if __name__ == "__main__":
    logger = MLFlowLogger(
        experiment_name="chess-eval", tracking_uri="sqlite:///mlflow.db"
    )
    model = ChessModel(in_feat=837)
    dataset = ChessDataset("chess_dataset.db", "evaluations")
    dataloader = DataLoader(dataset, batch_size=1024, num_workers=4)
    trainer = lightning.Trainer(
        max_epochs=1,
        accelerator="mps",
        logger=logger,
    )
    trainer.fit(model, dataloader)
