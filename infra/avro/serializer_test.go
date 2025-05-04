package avro

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockAvroSerializer struct {
	mock.Mock
}

func (m *MockAvroSerializer) Serialize(schema string, data interface{}) ([]byte, error) {
	args := m.Called(schema, data)
	return args.Get(0).([]byte), args.Error(1)
}

func TestNewSerializer(t *testing.T) {
	// Arrange
	mockSerializer := new(MockAvroSerializer)

	// Act
	serializer, err := NewSerializer(mockSerializer)

	// Assert
	assert.NoError(t, err)
	assert.NotNil(t, serializer)
	assert.Equal(t, mockSerializer, serializer.serializer)
}

func TestSerialize_Success(t *testing.T) {
	// Arrange
	mockSerializer := new(MockAvroSerializer)
	serializer := &Serializer{
		serializer: mockSerializer,
	}

	schema := "test-schema"
	data := struct {
		Name string
		Age  int
	}{
		Name: "John",
		Age:  30,
	}
	expectedBytes := []byte("serialized-data")

	mockSerializer.On("Serialize", schema, &data).Return(expectedBytes, nil)

	// Act
	result, err := serializer.Serialize(schema, &data)

	// Assert
	assert.NoError(t, err)
	assert.Equal(t, expectedBytes, result)
	mockSerializer.AssertExpectations(t)
}

func TestSerialize_Error(t *testing.T) {
	// Arrange
	mockSerializer := new(MockAvroSerializer)
	serializer := &Serializer{
		serializer: mockSerializer,
	}

	schema := "test-schema"
	data := struct {
		Name string
		Age  int
	}{
		Name: "John",
		Age:  30,
	}
	expectedError := errors.New("serialization error")

	mockSerializer.On("Serialize", schema, &data).Return([]byte{}, expectedError)

	// Act
	result, err := serializer.Serialize(schema, &data)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
	assert.Empty(t, result)
	mockSerializer.AssertExpectations(t)
}

func TestSerialize_WithPrimitiveTypes(t *testing.T) {
	// Arrange
	mockSerializer := new(MockAvroSerializer)
	serializer := &Serializer{
		serializer: mockSerializer,
	}

	testCases := []struct {
		name          string
		schema        string
		data          interface{}
		expectedBytes []byte
	}{
		{
			name:          "string data",
			schema:        "string-schema",
			data:          "string-data",
			expectedBytes: []byte("serialized-string"),
		},
		{
			name:          "integer data",
			schema:        "int-schema",
			data:          42,
			expectedBytes: []byte("serialized-int"),
		},
		{
			name:          "boolean data",
			schema:        "bool-schema",
			data:          true,
			expectedBytes: []byte("serialized-bool"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockSerializer.On("Serialize", tc.schema, tc.data).Return(tc.expectedBytes, nil).Once()

			// Act
			result, err := serializer.Serialize(tc.schema, tc.data)

			// Assert
			assert.NoError(t, err)
			assert.Equal(t, tc.expectedBytes, result)
		})
	}

	mockSerializer.AssertExpectations(t)
}
