# Data Dictionary

This document provides an overview of the structure and description of the data fields in this project.

## Sources

Here is the description of incoming sources used in this project. Majority of data are coming from chess.com public api.

### Table: Users

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| id          | Integer   | Unique identifier for each user |
| name        | String    | The name of the user |
| email       | String    | The email of the user |
| created_at  | DateTime  | The date and time when the user was created |

### Table: Games

| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| id          | Integer   | Unique identifier for each order |
| user_id     | Integer   | The ID of the user who placed the order |
| product_id  | Integer   | The ID of the product ordered |
| quantity    | Integer   | The quantity of the product ordered |
| created_at  | DateTime  | The date and time when the order was placed |


