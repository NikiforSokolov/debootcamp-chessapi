from app.assets.Chess import parse_game
import json

def test_game_parsing():
    with open('app_tests/assets/inputs/raw_game.txt', 'r') as file:
        test_input = json.loads(file.read())

    username = 'dolols'
    
    with open('app_tests/assets/inputs/parsed_game.txt', 'r') as file:
        expected_result = json.loads(file.read())

    assert parse_game(test_input, username) == expected_result
