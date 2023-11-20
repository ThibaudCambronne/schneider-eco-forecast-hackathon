import pandas as pd
import argparse

def load_data(file_path):
    # TODO: Load processed data from CSV file
    return df

def split_data(df):
    # convert an array of values into a dataset matrix
    def create_dataset(dataset, look_back=1, col=0):
        dataX, dataY = [], []
        for i in range(len(dataset)-look_back-1):
            a = dataset[i:(i+look_back), col]
            dataX.append(a)
            dataY.append(dataset[i + look_back, col])
        return np.array(dataX), np.array(dataY)
    
    # load the dataset
    dataset = df.values
    dataset = dataset.astype('float32')
    # split into train and test sets
    train_size = int(len(dataset) * 0.8)
    test_size = len(dataset) - train_size
    train, test = dataset[0:train_size, :], dataset[train_size:len(dataset), :]
    # reshape dataset
    look_back = 5
    trainX, trainY = create_dataset(train, look_back, col)
    testX, testY = create_dataset(test, look_back, col)
    return X_train, X_val, y_train, y_val

def train_model(X_train, y_train):
    # TODO: Initialize your model and train it
    return model

def save_model(model, model_path):
    # TODO: Save your trained model
    pass

def parse_arguments():
    parser = argparse.ArgumentParser(description='Model training script for Energy Forecasting Hackathon')
    parser.add_argument(
        '--input_file', 
        type=str, 
        default='data/processed_data.csv', 
        help='Path to the processed data file to train the model'
    )
    parser.add_argument(
        '--model_file', 
        type=str, 
        default='models/model.pkl', 
        help='Path to save the trained model'
    )
    return parser.parse_args()

def main(input_file, model_file):
    df = load_data(input_file)
    X_train, X_val, y_train, y_val = split_data(df)
    model = train_model(X_train, y_train)
    save_model(model, model_file)

if __name__ == "__main__":
    args = parse_arguments()
    main(args.input_file, args.model_file)