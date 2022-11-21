from Estimator_utils import deepctr_model_fn, DNN_SCOPE_NAME, variable_scope
import tensorflow as tf
from deepFM import FM, embedding_layer, dnn_layer, Liner
from Estimator_layer_utils import concat_func, combined_dnn_input
from Estimator_feature_columns import get_linear_logit, input_from_feature_columns


def DeepFMEstimator(linear_feature_columns, dnn_feature_columns, dnn_hidden_units=(256, 128, 64),
                    l2_reg_linear=0.00001, l2_reg_embedding=0.00001, l2_reg_dnn=0, seed=1024, dnn_dropout=0,
                    dnn_activation='relu', dnn_use_bn=False, task='binary', model_dir=None, config=None,
                    linear_optimizer='Ftrl', FM_k = 5,
                    dnn_optimizer='Adagrad', training_chief_hooks=None):

    def _model_fn(features, labels, mode, config):
        train_flag = (mode == tf.estimator.ModeKeys.TRAIN)

        linear_logits = get_linear_logit(features, linear_feature_columns, l2_reg=l2_reg_linear)

        with variable_scope(DNN_SCOPE_NAME):
            sparse_embedding_list, dense_value_list = input_from_feature_columns(features, dnn_feature_columns,
                                                                                l2_reg=l2_reg_embedding)


            dnn_input = combined_dnn_input(sparse_embedding_list, dense_value_list)

            fm_logit = FM()(concat_func(sparse_embedding_list, axis=1))

            dnn_output = dnn_layer(dnn_hidden_units, dnn_activation, 5)(emb, training=train_flag)
            dnn_logit = tf.keras.layers.Dense(
                1, use_bias=False, kernel_initializer=tf.keras.initializers.glorot_normal(seed=seed))(dnn_output)

        logits = linear_logits + fm_logit + dnn_logit

        return deepctr_model_fn(features, mode, logits, labels, task, linear_optimizer, dnn_optimizer,
                                training_chief_hooks
                                =training_chief_hooks)

    return tf.estimator.Estimator(_model_fn, model_dir=model_dir, config=config)
