import tensorflow as tf
from tensorflow.keras.layers import Layer, Dense, Embedding
from tensorflow.keras.models import Model
import numpy as np
import pandas as pd
from tensorflow.keras.layers import Concatenate, Flatten


class FM(Layer):
    def __init__(self, k, w_reg=0.0001, v_reg=0.0001):
        super(FM, self).__init__()
        self.vector_dim = k
        self.w_reg = w_reg
        self.v_reg = v_reg

    def build(self, input_shape):
        self.w0 = self.add_weight(name = 'w0', shape = (1,), initializer = tf.zeros_initializer(), trainable = True)
        self.w = self.add_weight(name='w', shape=(input_shape[-1], 1), initializer=tf.random_normal_initializer(), trainable=True, regularizer = tf.keras.regularizers.l2(self.w_reg))
        self.v = self.add_weight(name='v', shape=(input_shape[-1], self.vector_dim), initializer=tf.random_normal_initializer(),
                                 trainable=True, regularizer = tf.keras.regularizers.l2(self.v_reg))



    def call(self, input):
        liner_part = tf.matmul(input, self.w) + self.w0
        inter_part1 = tf.pow(tf.matmul(input, self.v), 2)
        inter_part2 = tf.matmul(tf.pow(input, 2), tf.pow(self.v, 2))
        inter_part = 0.5*tf.reduce_sum(inter_part1 - inter_part2, axis = -1, keepdims=True)
        output = liner_part + inter_part

        return output

class embedding_layer(Layer):
    def __init__(self, emb_type, feature_info):
        super(embedding_layer, self).__init__()
        self.emb_layer = Embedding(feature_info.vocabulary_size, feature_info.embedding_dim, emb_type + '_' + feature_info.name)

    def call(self, input):
        return self.emb_layer(input)

class dnn_layer(Layer):
    def __init__(self, unit, activation, output_dim):
        super(dnn_layer, self).__init__()
        self.unit = unit
        self.activation = activation
        self.output_dim = output_dim
        self.dense_list = [Dense(i, activation = self.activation) for i in self.unit]
        self.output_layer = Dense(self.output_dim, activation = None)
    def call(self, input):
        x = input
        for dense in self.dense_list:
            x = dense(x)
        output = self.output_layer(x)
        return output


class Liner(Layer):
    def __init__(self):
        super(Liner, self).__init__()
        self.out_layer = Dense(1, activation = None, use_bias=True)

    def call(self, input):
        out = self.out_layer(input)
        return out


class DFM(Layer):
    def __init__(self, spars_feature, units, dnn_out_dim, FM_k, FM_w_reg, FM_v_reg):
        super(DFM, self).__init__()
        self.FM = FM(FM_k, FM_w_reg, FM_v_reg)
        self.emb = Dense(15, activation = None)
        self.dense1 = Dense(1, activation = None)
        self.dnn = dnn_layer(units, tf.nn.relu, output_dim=dnn_out_dim)

    def call(self, input):
        emb_output = self.emb(input)
        FM_output = self.FM(emb_output)
        dnn_output = self.dnn(emb_output)
        #dnn_output = tf.keras.layers.Flatten()(dnn_output)
        model = Concatenate(axis=1)([FM_output, dnn_output])
        return self.dense1(model)


class model(Model):
    def __init__(self, spars_feature, units, dnn_out_dim, FM_k, FM_w_reg, FM_v_reg):
        super(model, self).__init__()
        self.base_model = DFM(spars_feature, units, dnn_out_dim, FM_k, FM_w_reg, FM_v_reg)

    def call(self, input):
        model_out = self.base_model(input)
        return tf.nn.sigmoid(model_out)
